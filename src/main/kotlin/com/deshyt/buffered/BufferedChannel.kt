package com.deshyt.buffered

import com.deshyt.Channel
import kotlinx.atomicfu.AtomicLong
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.loop
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine

class BufferedChannel<E>(private val capacity: Long) : Channel<E> {
    /**
      The counters show the total amount of senders and receivers ever performed. They are
      incremented in the beginning of the corresponding operation, thus acquiring a unique
      (for the operation type) cell to process.
     */
    private val sendersCounter = atomic(0L)
    private val receiversCounter = atomic(0L)

    /**
       This counter indicates the end of the channel buffer. Its value increases every time
       when a `receive` request is completed.
     */
    private val bufferEnd = atomic(capacity)

    /**
       This is the counter of completed [expandBuffer] invocations. It is used for maintaining
       the guarantee that [expandBuffer] is invoked on every cell.
       Initially, its value is equal to the buffer capacity.
     */
    private val completedExpandBuffers = atomic(capacity)

    /**
       These channel pointers indicate segments where values of [sendersCounter], [receiversCounter]
       and [bufferEnd] are currently located.
     */
    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>
    private val bufferEndSegment: AtomicRef<ChannelSegment<E>>

    init {
        require(capacity > 0) { "Invalid capacity ($capacity) for a buffered channel, should be >= 1." }
        val firstSegment = ChannelSegment(id = 0, prevSegment = null, channel = this)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
        bufferEndSegment = atomic(firstSegment.findSegment(bufferEnd.value / SEGMENT_SIZE))
    }

    override suspend fun send(elem: E) {
        // Read the segment reference before the counter increment; the order is crucial,
        // otherwise there is a chance the required segment will not be found
        var segment = sendSegment.value
        while (true) {
            // Obtain the global index for this sender right before
            // incrementing the `senders` counter.
            val s = sendersCounter.getAndIncrement()
            // Count the required segment id and the cell index in it.
            val id = s / SEGMENT_SIZE
            val index = (s % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // one (in the beginning of this function) has lower id.
            if (segment.id != id) {
                // Find the required segment.
                segment = findSegmentSend(id, segment) ?:
                    // The required segment has not been found, since it was full of cancelled
                    // cells and, therefore, physically removed. Restart the sender.
                    continue
            }
            // Place the element in the cell.
            segment.setElement(index, elem)
            // Update the cell according to the algorithm. If the cell was poisoned or
            // stores an interrupted receiver, clean the cell and restart the sender.
            if (updateCellOnSend(s, segment, index)) return
            segment.cleanElement(index)
        }
    }

    private suspend fun updateCellOnSend(
        /* The global index of the cell. */
        s: Long,
        /* The working cell is specified by the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int
    ): Boolean {
        while (true) {
            when (val state = segment.getState(index)) {
                // The cell is empty.
                null -> {
                    // If the cell is in the buffer or a receiver is coming, try to buffer
                    // the element. Otherwise, try to place the sleeping sender in the cell.
                    if (bufferOrRendezvousSend(s)) {
                        // Move the cell state to `BUFFERED`.
                        if (segment.casState(index, null, CellState.BUFFERED)) {
                            // The element has been successfully buffered, finish.
                            return true
                        }
                    } else {
                        // The sender should suspend.
                        if (trySuspendSender(segment, index)) return true
                    }
                }
                // The cell is in the logical buffer => try to buffer the element
                CellState.IN_BUFFER -> {
                    if (segment.casState(index, state, CellState.BUFFERED)) return true
                }
                // The cell was poisoned by a receiver => restart the sender
                CellState.POISONED -> return false
                // Cancelled receiver => restart the sender
                CellState.INTERRUPTED_RCV -> return false
                // Suspended receiver in the cell => try to resume it
                else -> {
                    val receiver = (state as? Coroutine)?.cont ?: (state as CoroutineEB).cont
                    if (receiver.tryResumeRequest(true)) {
                        segment.setState(index, CellState.DONE_RCV)
                        return true
                    } else {
                        // The resumption has failed, since the receiver was cancelled.
                        // Clean the cell and wait until `expandBuffer()`-s invoked on the cells
                        // before the current one finish.
                        segment.onCancellation(index = index, isSender = false)
                        return false
                    }
                }
            }
        }
    }

    override suspend fun receive(): E {
        // Read the segment reference before the counter increment; the order is crucial,
        // otherwise there is a chance the required segment will not be found
        var segment = receiveSegment.value
        while (true) {
            // Obtain the global index for this receiver right before
            // incrementing the `receivers` counter.
            val r = receiversCounter.getAndIncrement()
            // Count the required segment id and the cell index in it.
            val id = r / SEGMENT_SIZE
            val index = (r % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // one (in the beginning of this function) has lower id.
            if (segment.id != id) {
                // Find the required segment.
                segment = findSegmentReceive(id, segment) ?:
                    // The required segment has not been found, since it was full of cancelled
                    // cells and, therefore, physically removed. Restart the receiver.
                    continue
            }
            // Update the cell according to the algorithm. If the rendezvous happened,
            // the received value is returned, then the cell is cleaned to avoid memory
            // leaks. Otherwise, the receiver restarts.
            if (updateCellOnReceive(r, segment, index)) {
                return segment.retrieveElement(index)
            }
        }
    }

    private suspend fun updateCellOnReceive(
        /* The global index of the cell. */
        r: Long,
        /* The working cell is specified by the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int
    ): Boolean {
        while (true) {
            when (val state = segment.getState(index)) {
                // The cell is empty.
                null, CellState.IN_BUFFER -> {
                    // If a rendezvous must happen, the operation does not wait
                    // until the cell stores a buffered element or a suspended
                    // sender, poisoning the cell and restarting instead.
                    // Otherwise, try to store the specified waiter in the cell.
                    val s = sendersCounter.value
                    if (r < s) {
                        // The cell is already covered by sender, so a rendezvous must happen.
                        // Unfortunately, the cell is empty, so the operation poisons it.
                        if (segment.casState(index, state, CellState.POISONED)) {
                            // When the cell becomes poisoned, expand the logical buffer and restart.
                            expandBuffer()
                            return false
                        }
                    } else {
                        // The receiver should suspend.
                        if (trySuspendReceiver(segment, index)) return true
                    }
                }
                // Buffered element => finish
                CellState.BUFFERED -> {
                    segment.setState(index, CellState.DONE_RCV).also { expandBuffer() }
                    return true
                }
                // Cancelled sender => restart
                CellState.INTERRUPTED_SEND -> return false
                // `expandBuffer()` is resuming the sender => wait
                CellState.RESUMING_BY_EB -> continue
                // Suspended sender in the cell => try to resume it
                else -> {
                    // To synchronize with expandBuffer(), the algorithm first moves the cell to an
                    // intermediate `RESUMING_BY_RCV` state, updating it to either `DONE_RCV` (on success)
                    // or `INTERRUPTED_SEND` (on failure).
                    if (segment.casState(index, state, CellState.RESUMING_BY_RCV)) {
                        // Has a concurrent `expandBuffer()` delegated its completion?
                        val helpExpandBuffer = state is CoroutineEB
                        // Extract the sender's coroutine and try to resume it
                        val sender = (state as? Coroutine)?.cont ?: (state as CoroutineEB).cont
                        return if (sender.tryResumeRequest(true)) {
                            // The sender was resumed successfully. Update the cell state, expand the buffer and finish.
                            // In case a concurrent `expandBuffer()` has delegated its completion, the procedure should
                            // finish, as the sender is resumed. Thus, no further action is required.
                            segment.setState(index, CellState.DONE_RCV).also { expandBuffer() }
                            true
                        } else {
                            // The resumption has failed. Update the cell state and restart the receiver.
                            // In case a concurrent `expandBuffer()` has delegated its completion, the procedure should
                            // skip this cell, so `expandBuffer()` should be called once again.
                            segment.onCancellation(index = index, isSender = true)
                            if (helpExpandBuffer) expandBuffer()
                            false
                        }
                    }
                }
            }
        }
    }

    /**
       This method suspends a sender. If the sender's suspended coroutine is successfully placed
       in the cell, the method returns true. Otherwise, the coroutine is resumed and the method
       returns false, thus restarting the sender.
     */
    private suspend fun trySuspendSender(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation {
                segment.onCancellation(index = index, isSender = true)
            }
            if (!segment.casState(index, null, Coroutine(cont))) {
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
            }
        }
    }

    /**
       This method suspends a receiver. If the suspended receiver is successfully placed in the cell,
       the method invokes [expandBuffer] and returns true. Otherwise, the coroutine is resumed and
       the method returns false, thus restarting the receiver.
     */
    private suspend fun trySuspendReceiver(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation {
                segment.onCancellation(index = index, isSender = false)
            }
            if (segment.casState(index, null, Coroutine(cont))) {
                expandBuffer()
            } else {
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
            }
        }
    }

    /**
       Responsible for resuming a coroutine. The given value is the one that should be returned
       in the suspension point. If the coroutine is successfully resumed, the method returns true,
       otherwise it returns false.
     */
    @OptIn(InternalCoroutinesApi::class)
    private fun <T> CancellableContinuation<T>.tryResumeRequest(value: T): Boolean {
        val token = tryResume(value)
        return if (token != null) {
            completeResume(token)
            true
        } else {
            false
        }
    }

    /**
        This method finds the segment which contains non-interrupted cells and which id >= the
        requested [id]. In case the required segment has not been created yet, this method attempts
        to add it to the underlying linked list. Finally, it updates [sendSegment] to the found
        segment if its [ChannelSegment.id] is greater than the one of the already stored segment.

        In case the requested segment is already removed, the method returns `null`.
     */
    private fun findSegmentSend(id: Long, startFrom: ChannelSegment<E>): ChannelSegment<E>? {
        val segment = sendSegment.findSegmentAndMoveForward(id, startFrom)
        return if (segment.id > id) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [sendersCounter].
            sendersCounter.updateCounterIfLower(segment.id * SEGMENT_SIZE)
            // As the required segment is already removed, return `null`.
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    /**
        This method finds the segment which contains non-interrupted cells and which id >= the
        requested [id]. In case the required segment has not been created yet, this method attempts
        to add it to the underlying linked list. Finally, it updates [receiveSegment] to the found
        segment if its [ChannelSegment.id] is greater than the one of the already stored segment.

        In case the requested segment is already removed, the method returns `null`.
     */
    private fun findSegmentReceive(id: Long, startFrom: ChannelSegment<E>): ChannelSegment<E>? {
        val segment = receiveSegment.findSegmentAndMoveForward(id, startFrom)
        return if (segment.id > id) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [receiversCounter].
            receiversCounter.updateCounterIfLower(segment.id * SEGMENT_SIZE)
            // As the required segment is already removed, return `null`.
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    /**
        Updates the counter ([sendersCounter] or [receiversCounter]) if its value is lower than
        the specified one. The method is used to efficiently skip a sequence of cancelled cells
        in [findSegmentSend] and [findSegmentReceive].
     */
    @Suppress("NOTHING_TO_INLINE")
    private inline fun AtomicLong.updateCounterIfLower(value: Long): Unit =
        loop { curCounter ->
            if (curCounter >= value) return
            if (compareAndSet(curCounter, value)) return
        }

    /**
       This method returns the first segment which contains non-interrupted cells and which
       id >= the required [id]. In case the required segment has not been created yet, the
       method creates new segments and adds them to the underlying linked list.
       After the desired segment is found and the `AtomicRef` pointer is successfully moved
       to it, the segment is returned by the method.
     */
    @Suppress("NOTHING_TO_INLINE")
    internal inline fun <E> AtomicRef<ChannelSegment<E>>.findSegmentAndMoveForward(
        id: Long,
        startFrom: ChannelSegment<E>
    ): ChannelSegment<E> {
        while (true) {
            val dest = startFrom.findSegment(id)
            // Try to update `value` and restart if the found segment is logically removed
            if (moveForward(dest)) return dest
        }
    }

    /**
       This method helps moving the `AtomicRef` pointer forward.
       If the pointer is being moved to the segment which is logically removed, the method
       returns false, thus forcing [findSegmentAndMoveForward] method to restart.
     */
    @Suppress("NOTHING_TO_INLINE")
    internal inline fun <E> AtomicRef<ChannelSegment<E>>.moveForward(to: ChannelSegment<E>): Boolean = loop { cur ->
        if (cur.id >= to.id) {
            // No need to update the pointer, it was already updated by another request.
            return true
        }
        if (to.isRemoved) {
            // Trying to move pointer to the segment which is logically removed.
            // Restart [AtomicRef<S>.findAndMoveForward].
            return false
        }
        if (compareAndSet(cur, to)) {
            // The segment was successfully moved.
            cur.tryRemoveSegment()
            return true
        }
    }

    /**
        Returns `true` when the specified [send] should place its element to the working
        cell without suspension.
     */
    private fun bufferOrRendezvousSend(curSenders: Long): Boolean =
        curSenders < bufferEnd.value || curSenders < receiversCounter.value + capacity

    /**
       This method is responsible for updating the [bufferEnd] counter. It is called after
       `receive()` successfully performs its synchronization, either retrieving the first
       element, or storing its coroutine for suspension.
     */
    private fun expandBuffer() {
        // Read the current segment of the `expandBuffer()` procedure.
        var segment = bufferEndSegment.value
        // Try to expand the buffer until succeed.
        while (true) {
            // Increment the logical end of the buffer.
            // The `b`-th cell is going to be added to the buffer.
            val b = bufferEnd.getAndIncrement()
            val id = b / SEGMENT_SIZE
            // After that, read the current `senders` counter. In case its value is lower than `b`,
            // the `send(e)` invocation that will work with this `b`-th cell will detect that the
            // cell is already a part of the buffer when comparing with the `bufferEnd` counter.
            if (b >= sendersCounter.value) {
                // The cell is not covered by send() request. Increment the number of
                // completed `expandBuffer()`-s and finish.
                incCompletedExpandBufferAttempts()
                return
            }
            // Is `bufferEndSegment` outdated or is the segment with the required id already removed?
            // Find the required segment, creating new ones if needed.
            if (segment.id != id) {
                segment = findSegmentBufferEnd(id, segment, b) ?:
                    // The required segment has been removed, restart `expandBuffer()`.
                    continue
            }
            // Try to add the cell to the logical buffer, updating the cell state
            // according to the algorithm.
            val index = (b % SEGMENT_SIZE).toInt()
            if (updateCellOnExpandBuffer(segment, index, b)) {
                // The cell has been added to the logical buffer.
                // Increment the number of completed `expandBuffer()`-s and finish.
                incCompletedExpandBufferAttempts()
                return
            } else {
                // The cell has not been added to the buffer. Increment the number of
                // completed `expandBuffer()` attempts and restart.
                incCompletedExpandBufferAttempts()
                continue
            }
        }
    }

    private fun findSegmentBufferEnd(id: Long, startFrom: ChannelSegment<E>, currentBufferEndCounter: Long)
    : ChannelSegment<E>? {
        val segment = bufferEndSegment.findSegmentAndMoveForward(id, startFrom)
        return if (segment.id > id) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [bufferEnd] counter.
            if (bufferEnd.compareAndSet(currentBufferEndCounter + 1, segment.id * SEGMENT_SIZE)) {
                incCompletedExpandBufferAttempts(segment.id * SEGMENT_SIZE - currentBufferEndCounter)
            } else {
                incCompletedExpandBufferAttempts()
            }
            // As the required segment is already removed, return `null`.
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    /**
       This method returns true if [expandBuffer] should finish and false if [expandBuffer] should restart.
     */
    private fun updateCellOnExpandBuffer(segment: ChannelSegment<E>, index: Int, b: Long): Boolean {
        while (true) {
            when (val state = segment.getState(index)) {
                // The cell is empty => mark the cell as "in the buffer" and finish
                null -> if (segment.casState(index, null, CellState.IN_BUFFER)) return true
                // A suspended coroutine, sender or receiver
                is Coroutine -> {
                    if (b >= receiversCounter.value) {
                        // Suspended sender, since the cell is not covered by a receiver. Try to resume it.
                        if (segment.casState(index, state, CellState.RESUMING_BY_EB)) {
                            return if (state.cont.tryResumeRequest(true)) {
                                segment.setState(index, CellState.BUFFERED)
                                true
                            } else {
                                segment.onCancellation(index = index, isSender = true)
                                false
                            }
                        }
                    }
                    // Cannot distinguish the coroutine, add `EB` marker to it
                    if (segment.casState(index, state, CoroutineEB(state.cont))) return true
                }
                // The element is already buffered => finish
                CellState.BUFFERED -> return true
                // The rendezvous happened => finish, expandBuffer() was invoked before the receiver was suspended
                CellState.DONE_RCV -> return true
                // The sender was interrupted => restart
                CellState.INTERRUPTED_SEND -> return false
                // The receiver was interrupted => finish, expandBuffer() was invoked before the receiver was suspended
                CellState.INTERRUPTED_RCV -> return true
                // Poisoned cell => finish, the receiver that poisoned the cell is in charge
                CellState.POISONED -> return true
                // A receiver is resuming the sender => wait in a spin loop until it changes
                // the state to either `DONE_RCV` or `INTERRUPTED_SEND`
                CellState.RESUMING_BY_RCV -> continue
                else -> error("Unexpected cell state: $state")
            }
        }
    }

    /**
       Increases the amount of completed [expandBuffer] invocations.
     */
    private fun incCompletedExpandBufferAttempts(nAttempts: Long = 1) {
        completedExpandBuffers.addAndGet(nAttempts)
    }

    /**
       Waits in a spin-loop until the [expandBuffer] call that should process the [globalIndex]-th
       cell is completed. Essentially, it waits until the numbers of started ([bufferEnd]) and
       completed ([completedExpandBuffers]) [expandBuffer] attempts coincide and become equal or
       greater than [globalIndex].
     */
    internal fun waitExpandBufferCompletion(globalIndex: Long) {
        // Wait in an infinite loop until the number of started buffer expansion calls
        // become not lower than the cell index.
        @Suppress("ControlFlowWithEmptyBody")
        while (bufferEnd.value <= globalIndex) {}
        // Now it is guaranteed that the `expandBuffer()` call that should process the
        // required cell has been started. Wait in an infinite loop until the numbers of
        // started and completed buffer expansion calls coincide.
        while (true) {
            val b = bufferEnd.value
            val completedEB = completedExpandBuffers.value
            // Do the numbers of started and completed [expandBuffer]-s coincide?
            // The comparison with [bufferEnd] guarantees that, if `b == completedEB`, then all
            // [expandBuffer]-s invoked on the cells before the [globalIndex]-th one have finished.
            if (b == completedEB && b == bufferEnd.value) return
        }
    }

    // ###################################
    // # Validation of the channel state #
    // ###################################

    override fun checkSegmentStructureInvariants() {
        val firstSegment = getFirstSegment()

        /* TODO Check that the `prev` link of the leftmost segment is correct. The link can be non-null, if it points
                to the segment which cells were all used by the requests and not all of them were interrupted. */
//        val prev = firstSegment.getPrev()
//        check(prev == null || !prev.isRemoved()) { "Channel $this: the `prev` link of the leftmost segment is not null." }

        var curSegment: ChannelSegment<E>? = firstSegment
        while (curSegment != null) {
            // Check that the segment's `prev` link is correct
            if (curSegment != firstSegment) {
                check(curSegment.getPrev() != null) { "Channel $this: `prev` link is null, but the segment $curSegment is not the leftmost one." }
                check(
                    curSegment.getPrev()!!.getNext() == curSegment
                ) { "Channel $this: `prev` points to the wrong segment for $curSegment." }
            }

            // TODO Check that the removed segments are not reachable from the list.
            // The tail segment cannot be removed physically. Otherwise, uniqueness of segment id is not guaranteed.
//            check(curSegment.isAlive() || curSegment.getNext() == null) { "Channel $this: the segment $curSegment is marked removed, but is reachable from the segment list." }

            // Check that the segment's state is correct
            curSegment.validate()

            curSegment = curSegment.getNext()
        }
    }

    /*
       This method returns the leftmost segment in the segment list.
       It handles the situation when one pointer was moved further, the other pointer remained
       the same and segments between the pointers were removed:

                         REMOVED             REMOVED
                        ┌───────┐ ────────► ┌───────┐ ────────► ┌───────┐
                        └───────┘ null◄──── └───────┘ null◄──── └───────┘
                            ▲                                       ▲
                            │                                       │
                         `receiveSegment`                        `sendSegment`

       In this case, the leftmost segment is the first alive one or the tail.
     */
    private fun getFirstSegment(): ChannelSegment<E> {
        var cur = listOf(receiveSegment.value, sendSegment.value).minBy { it.id }
        while (cur.isRemoved && cur.getNext() != null) {
            cur = cur.getNext()!!
        }
        return cur
    }
}

/**
   A waiter that stores a suspended coroutine. The process of resumption does not depend on
   which suspended request (a sender or a receiver) is stored in the cell.
 */
internal data class Coroutine(val cont: CancellableContinuation<Boolean>)

/**
   A waiter that stores a suspended coroutine with the `EB` marker. The marker is added when [expandBuffer]
   cannot distinguish whether the coroutine stored in the cell is a suspended sender or receiver. Thus, the
   [expandBuffer] completion is delegated to a request of the opposite type which will come to the cell
   in the future.
 */
internal data class CoroutineEB(val cont: CancellableContinuation<Boolean>)
