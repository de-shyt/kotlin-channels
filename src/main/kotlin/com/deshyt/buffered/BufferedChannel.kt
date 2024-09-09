package com.deshyt.buffered

import com.deshyt.Channel
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.loop
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine

class BufferedChannel<E>(capacity: Long) : Channel<E> {
    /*
      The counters show the total amount of senders and receivers ever performed. They are
      incremented in the beginning of the corresponding operation, thus acquiring a unique
      (for the operation type) cell to process.
     */
    private val sendersCounter = atomic(0L)
    private val receiversCounter = atomic(0L)

    /*
       This counter indicates the end of the channel buffer. Its value increases every time
       when a `receive` request is completed.
     */
    private val bufferEnd = atomic(capacity)

    /*
       The channel pointers indicate segments where values of `sendersCounter` and
       `receiversCounter` are currently located.
     */
    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>
    private val bufferEndSegment: AtomicRef<ChannelSegment<E>>

    init {
        val firstSegment = ChannelSegment<E>(id = 0, prevSegment = null)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
        bufferEndSegment = atomic(firstSegment.findSegment(bufferEnd.value / SEGMENT_SIZE))
    }

    override suspend fun send(elem: E) {
        // Read the segment reference before the counter increment;
        // it is crucial to be able to find the required segment later.
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
                        // The required segment has not been found, since it was full of
                        // cancelled cells and, therefore, physically removed.
                        // Restart the sender.
                        continue
            }
            // Place the element in the cell.
            segment.setElement(index, elem)
            // Update the cell according to the algorithm. If the cell was
            // poisoned or stores an interrupted receiver, clean the cell
            // and restart the sender.
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
            val state = segment.getState(index)
            val b = bufferEnd.value
            val r = receiversCounter.value
            when {
                // Empty and either the cell is in the buffer or a receiver is coming => buffer
                state == null && (s < r || s < b) || state == CellState.IN_BUFFER -> {
                    if (segment.casState(index, state, CellState.BUFFERED)) return true
                }
                // Empty, the cell is not in the buffer and no receiver is coming => suspend
                state == null && s >= b && s >= r -> {
                    if (trySuspendSender(segment, index)) return true
                }
                // The cell was poisoned by a receiver => restart the sender
                state == CellState.POISONED -> return false
                // Cancelled receiver => restart the sender
                state == CellState.INTERRUPTED_RCV -> return false
                // Suspended receiver in the cell => try to resume it
                else -> {
                    val receiver = (state as? Coroutine)?.cont ?: (state as CoroutineEB).cont
                    if (receiver.tryResumeRequest(true)) {
                        segment.casState(index, state, CellState.DONE_RCV)
                        return true
                    } else {
                        // Receiver was cancelled
                        return false
                    }
                }
            }
        }
    }

    override suspend fun receive(): E {
        // Read the segment reference before the counter increment;
        // it is crucial to be able to find the required segment later.
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
                    // The required segment has not been found, since it was full of
                    // cancelled cells and, therefore, physically removed.
                    // Restart the receiver.
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
            val state = segment.getState(index)
            val s = sendersCounter.value
            when {
                // The cell is empty and no sender is coming => suspend
                (state == null || state == CellState.IN_BUFFER) && r >= s -> {
                    if (trySuspendReceiver(segment, index)) return true
                }
                // The cell is empty but a sender is coming => poison & restart
                (state == null || state == CellState.IN_BUFFER) && r < s -> {
                    if (segment.casState(index, state, CellState.POISONED)) {
                        expandBuffer()
                        return false
                    }
                }
                // Buffered element => finish
                state == CellState.BUFFERED -> {
                    expandBuffer()
                    return true
                }
                // Cancelled sender => restart
                state == CellState.INTERRUPTED_SEND -> return false
                // `expandBuffer()` is resuming the sender => wait
                state == CellState.RESUMING_BY_EB -> continue
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
                        if (sender.tryResumeRequest(true)) {
                            // The sender was resumed successfully. Update the cell state, expand the buffer and finish.
                            // In case a concurrent `expandBuffer()` has delegated its completion, the procedure should
                            // finish, as the sender is resumed. Thus, no further action is required.
                            segment.setState(index, CellState.DONE_RCV)
                            expandBuffer()
                            return true
                        } else {
                            // The resumption has failed. Update the cell state and restart the receiver.
                            // In case a concurrent `expandBuffer()` has delegated its completion, the procedure should
                            // skip this cell, so `expandBuffer()` should be called once again.
                            segment.setState(index, CellState.INTERRUPTED_SEND)
                            if (helpExpandBuffer) expandBuffer()
                            return false
                        }
                    }
                }
            }
        }
    }

    /*
       This method suspends a sender. If the sender's suspended coroutine is successfully placed
       in the cell, the method returns true. Otherwise, the coroutine is resumed and the method
       returns false, thus restarting the sender.
     */
    private suspend fun trySuspendSender(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation {
                segment.onCancellation(index = index, newState = CellState.INTERRUPTED_SEND)
            }
            if (!segment.casState(index, null, Coroutine(cont))) {
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
            }
        }
    }

    /*
       This method suspends a receiver. If the receiver's suspended coroutine is successfully
       placed in the cell, the method invokes [expandBuffer] and returns true. Otherwise, the
       coroutine is resumed and the method returns false, thus restarting the receiver.
     */
    private suspend fun trySuspendReceiver(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation {
                segment.onCancellation(index = index, newState = CellState.INTERRUPTED_RCV)
            }
            if (segment.casState(index, null, Coroutine(cont))) {
                expandBuffer()
            } else {
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
            }
        }
    }

    /*
       Responsible for resuming a coroutine. The given value is the one that should be returned
       in the suspension point. If the coroutine is successfully resumed, [tryResumeRequest]
       returns true, otherwise it returns false.
     */
    @OptIn(InternalCoroutinesApi::class)
    private fun <T> CancellableContinuation<T>.tryResumeRequest(value: T): Boolean {
        val token = tryResume(value)
        if (token != null) {
            completeResume(token)
            return true
        } else {
            return false
        }
    }

    private fun findSegmentSend(requiredId: Long, startFrom: ChannelSegment<E>): ChannelSegment<E>? {
        val segment = findAndMoveForwardSend(requiredId, startFrom)
        return if (segment.id > requiredId) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [sendersCounter].
            updateSendersCounterIfLower(segment.id * SEGMENT_SIZE)
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    private fun findSegmentReceive(requiredId: Long, startFrom: ChannelSegment<E>): ChannelSegment<E>? {
        val segment = findAndMoveForwardReceive(requiredId, startFrom)
        return if (segment.id > requiredId) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [receiversCounter].
            updateReceiversCounterIfLower(segment.id * SEGMENT_SIZE)
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    /*
       Updates the `senders` counter if its value is lower that the specified one.
       Senders use this method to efficiently skip a sequence of cancelled receivers.
     */
    private fun updateSendersCounterIfLower(value: Long): Unit =
        sendersCounter.loop { curCounter ->
            if (curCounter >= value) return
            if (sendersCounter.compareAndSet(curCounter, value)) return
        }

    /*
       Updates the `receivers` counter if its value is lower that the specified one.
       Receivers use this method to efficiently skip a sequence of cancelled senders.
     */
    private fun updateReceiversCounterIfLower(value: Long): Unit =
        receiversCounter.loop { curCounter ->
            if (curCounter >= value) return
            if (receiversCounter.compareAndSet(curCounter, value)) return
        }

    /*
       These methods return the first segment which contains non-interrupted cells and which
       id >= the required id. Depending on the request type, either `sendSegment` or
       `receiveSegment` pointer is updated.
     */
    private fun findAndMoveForwardSend(requiredId: Long, startFrom: ChannelSegment<E>): ChannelSegment<E> {
        while (true) {
            val segment = startFrom.findSegment(requiredId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardSend(segment)) {
                return segment
            }
        }
    }

    private fun findAndMoveForwardReceive(requiredId: Long, startFrom: ChannelSegment<E>): ChannelSegment<E> {
        while (true) {
            val segment = startFrom.findSegment(requiredId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardReceive(segment)) {
                return segment
            }
        }
    }

    /*
       These methods help moving the pointers forward.
       If the pointer is being moved to the segment which is logically removed, the method
       returns false, thus forcing [findAndMoveForwardSend] (or [findAndMoveForwardReceive])
       method to restart.
     */
    private fun moveForwardSend(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = sendSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another request.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardSend].
                return false
            }
            if (sendSegment.compareAndSet(cur, to)) {
                cur.tryRemoveSegment()
                return true
            }
        }
    }

    private fun moveForwardReceive(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = receiveSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another request.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardReceive].
                return false
            }
            if (receiveSegment.compareAndSet(cur, to)) {
                cur.tryRemoveSegment()
                return true
            }
        }
    }

    /*
       This method is responsible for updating the [bufferEnd] counter. It is called after
       `receive()` successfully performs its synchronization, either retrieving the first
       element, or storing its coroutine for suspension.
     */
    private fun expandBuffer() {
        // Read the current segment of
        // the `expandBuffer()` procedure.
        var segment = bufferEndSegment.value
        // Try to expand the buffer until succeed.
        try_again@ while (true) {
            // Increment the logical end of the buffer.
            // The `b`-th cell is going to be added to the buffer.
            val b = bufferEnd.getAndIncrement()
            val id = b / SEGMENT_SIZE
            // After that, read the current `senders` counter.
            // In case its value is lower than `b`, the `send(e)`
            // invocation that will work with this `b`-th cell
            // will detect that the cell is already a part of the
            // buffer when comparing with the `bufferEnd` counter.
            if (b >= sendersCounter.value) {
                // The cell is not covered by send() request. Increment the number
                // of completed `expandBuffer()`-s and finish.
                incCompletedExpandBufferAttempts()
                return
            }
            if (segment.id != id) {
                segment = findSegmentBufferEnd(id, segment, b) ?:
                    // The required segment has been removed, restart `expandBuffer()`.
                    continue@try_again
            }
            // Try to add the cell to the logical buffer, updating the cell
            // state according to the algorithm.
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
                continue@try_again
            }
        }
    }

    private fun findSegmentBufferEnd(id: Long, startFrom: ChannelSegment<E>, currentBufferEndCounter: Long)
    : ChannelSegment<E>? {
        val segment = findAndMoveForwardEB(startFrom, id)
        return if (segment.id > id) {
            // The required segment has been removed; `segment` is the first
            // segment with `id` not lower than the required one.
            // Skip the sequence of interrupted cells by updating [bufferEnd] counter.
            if (bufferEnd.compareAndSet(currentBufferEndCounter + 1, segment.id * SEGMENT_SIZE)) {
                incCompletedExpandBufferAttempts(segment.id * SEGMENT_SIZE - currentBufferEndCounter)
            } else {
                incCompletedExpandBufferAttempts()
            }
            null
        } else {
            // The required segment has been found, return it.
            segment
        }
    }

    private fun findAndMoveForwardEB(startFrom: ChannelSegment<E>, requiredId: Long): ChannelSegment<E> {
        while (true) {
            val segment = startFrom.findSegment(requiredId)
            // Try to update `bufferEndSegment` and restart if the found segment is logically removed
            if (moveForwardEB(segment)) {
                return segment
            }
        }
    }

    private fun moveForwardEB(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = bufferEndSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another `expandBuffer()` invocation.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardEB].
                return false
            }
            if (bufferEndSegment.compareAndSet(cur, to)) {
                return true
            }
        }
    }

    /*
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
                            if (state.cont.tryResumeRequest(true)) {
                                segment.setState(index, CellState.BUFFERED)
                                return true
                            } else {
                                segment.setState(index, CellState.INTERRUPTED_SEND)
                                return false
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
                // The receiver was interrupted => finish
                CellState.INTERRUPTED_RCV -> return true
                // Poisoned cell => finish, receive() is in charge
                CellState.POISONED -> return true
                // A receiver is resuming the sender => wait
                CellState.RESUMING_BY_RCV -> continue
            }
        }
    }

    /*
       Increases the amount of completed `expandBuffer` invocations.
     */
    private fun incCompletedExpandBufferAttempts(nAttempts: Long = 1) {
//        completedExpandBuffers.addAndGet(nAttempts)
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
        while (cur.isRemoved() && cur.getNext() != null) {
            cur = cur.getNext()!!
        }
        return cur
    }
}

/* A waiter that stores a suspended coroutine. The process of resumption does not depend on
   which suspended request (a sender or a receiver) is stored in the cell. */
internal data class Coroutine(val cont: CancellableContinuation<Boolean>)

/*
   A waiter that stores a suspended coroutine with the `EB` marker. The marker means [expandBuffer]
   needs to be completed depending on the resumption result. [expandBuffer] is invoked in case
   the suspended request is the sender which fails to resume.
*/
internal data class CoroutineEB(val cont: CancellableContinuation<Boolean>)
