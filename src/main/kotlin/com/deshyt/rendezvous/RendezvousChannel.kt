package com.deshyt.rendezvous

import com.deshyt.Channel
import kotlinx.atomicfu.AtomicLong
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.loop
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine

class RendezvousChannel<E> : Channel<E> {
    /**
       The counters show the total amount of senders and receivers ever performed. They are
       incremented in the beginning of the corresponding operation, thus acquiring a unique
       (for the operation type) cell to process.
     */
    private val sendersCounter = atomic(0L)
    private val receiversCounter = atomic(0L)

    /**
       These channel pointers indicate segments where values of [sendersCounter] and
       [receiversCounter] are currently located.
     */
    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>

    init {
        val firstSegment = ChannelSegment<E>(id = 0, prevSegment = null)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
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

    private suspend fun updateCellOnSend(s: Long, segment: ChannelSegment<E>, index: Int): Boolean {
        while (true) {
            if (s < receiversCounter.value) {
                when (segment.getState(index)) {
                    // Receiver covers the cell, but the cell is empty => try to buffer the element
                    null -> if (segment.casState(index, null, CellState.BUFFERED)) return true
                    // Suspended receiver in the cell => try to make a rendezvous
                    is CancellableContinuation<*> -> return tryMakeRendezvous(segment, index)
                    // The cell was poisoned by a receiver => restart the sender
                    CellState.POISONED -> return false
                    // Cancelled receiver in the cell => restart the sender
                    CellState.INTERRUPTED -> return false
                }
            } else {
                // The cell is empty => try to place the sender's coroutine in the cell.
                if (trySuspendRequest(segment, index)) return true
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

    private suspend fun updateCellOnReceive(r: Long, segment: ChannelSegment<E>, index: Int): Boolean {
        while (true) {
            if (r < sendersCounter.value) {
                when (segment.getState(index)) {
                    // The sender covers the cell, but the cell is empty => break the cell and restart the receiver
                    null -> if (segment.casState(index, null, CellState.POISONED)) return false
                    // Suspended sender is in the cell => try to make a rendezvous
                    is CancellableContinuation<*> -> return tryMakeRendezvous(segment, index)
                    // The element was placed in the cell by the sender => return the element
                    CellState.BUFFERED -> return true
                    // Cancelled sender in the cell => restart the receiver
                    CellState.INTERRUPTED -> return false
                }
            } else {
                // The cell is empty => try to place the receiver's coroutine in the cell
                if (trySuspendRequest(segment, index)) return true
            }
        }
    }

    /**
       Responsible for making a rendezvous. In case there is a suspended coroutine in the cell,
       if the coroutine is successfully resumed, the cell is marked DONE; otherwise, it is
       marked BROKEN and resources are cleaned.
     */
    @Suppress("UNCHECKED_CAST")
    private fun tryMakeRendezvous(segment: ChannelSegment<E>, index: Int): Boolean {
        val cont = segment.getState(index)
        if (cont is CancellableContinuation<*>) {
            if ((cont as CancellableContinuation<Boolean>).tryResumeRequest(true)) {
                segment.casState(index, cont, CellState.DONE)
                return true
            } else {
                segment.casState(index, cont, CellState.POISONED)
                return false
            }
        }
        // The cell state was changed to INTERRUPTED during the rendezvous
        return false
    }

    /**
       Responsible for suspending a request. When the opposite request comes to the cell, it
       resumes the coroutine with the specified boolean value. This value is then returned by
       [trySuspendRequest].
       If there is a failure while trying to place a coroutine in the cell, [trySuspendRequest]
       resumes the coroutine and returns false.
     */
    private suspend fun trySuspendRequest(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { segment.onCancellation(index) }
            // Try to place the coroutine in the cell.
            if (!segment.casState(index, null, cont)) {
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
            }
        }
    }

    /**
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
    internal inline fun <E> AtomicRef<ChannelSegment<E>>.moveForward(to: ChannelSegment<E>): Boolean =
        loop { cur ->
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

    // ###################################
    // # Validation of the channel state #
    // ###################################

    override fun checkSegmentStructureInvariants() {
        val firstSegment = getFirstSegment()

        /* Check that the `prev` link of the leftmost segment is correct. The link can be non-null, if it points
           to the segment which cells were all used by the requests and not all of them were interrupted. */
        val prev = firstSegment.getPrev()
        check(prev == null || !prev.isRemoved) { "Channel $this: the `prev` link of the leftmost segment is not null." }

        var curSegment: ChannelSegment<E>? = firstSegment
        while (curSegment != null) {
            // Check that the segment's `prev` link is correct
            if (curSegment != firstSegment) {
                check(curSegment.getPrev() != null) { "Channel $this: `prev` link is null, but the segment $curSegment is not the leftmost one." }
                check(curSegment.getPrev()!!.getNext() == curSegment) { "Channel $this: `prev` points to the wrong segment for $curSegment." }
            }

            // Check that the removed segments are not reachable from the list.
            // The tail segment cannot be removed physically. Otherwise, uniqueness of segment id is not guaranteed.
            check(!curSegment.isRemoved || curSegment.getNext() == null) { "Channel $this: the segment $curSegment is marked removed, but is reachable from the segment list." }

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