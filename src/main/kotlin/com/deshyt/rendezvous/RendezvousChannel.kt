package com.deshyt.rendezvous

import com.deshyt.Channel
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine

class RendezvousChannel<E> : Channel<E> {
    /*
      The counters show the total amount of senders and receivers ever performed.
      The counters are incremented in the beginning of the corresponding operation, thus
      acquiring a unique (for the operation type) cell to process.
     */
    private val sendersCounter = atomic(0L)
    private val receiversCounter = atomic(0L)

    /*
       The channel pointers indicate segments where values of `sendersCounter` and
       `receiversCounter` are currently located.
     */
    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>

    init {
        val firstSegment = ChannelSegment<E>(id = 0, prevSegment = null)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
    }

    override suspend fun send(elem: E) {
        while (true) {
            val startSegment = sendSegment.value
            val s = sendersCounter.getAndIncrement()
            val segment = findAndMoveForwardSend(startSegment = startSegment, destSegmentId = s / SEGMENT_SIZE)
            if (segment.id != s / SEGMENT_SIZE) {
                // The `sendSegment` pointer was updated. Skip the segment(s) with INTERRUPTED cells
                sendersCounter.compareAndSet(s + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            val index = (s % SEGMENT_SIZE).toInt()
            segment.setElement(index, elem)
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
        while (true) {
            val startSegment = receiveSegment.value
            val r = receiversCounter.getAndIncrement()
            val segment = findAndMoveForwardReceive(startSegment = startSegment, destSegmentId = r / SEGMENT_SIZE)
            if (segment.id != r / SEGMENT_SIZE) {
                // The `receiveSegment` pointer was updated. Skip the segment(s) with INTERRUPTED cells
                receiversCounter.compareAndSet(r + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            val index = (r % SEGMENT_SIZE).toInt()
            if (updateCellOnReceive(r, segment, index)) {
                // The element was successfully received. Return the value and clean the cell to avoid memory leaks
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

    /*
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

    /*
       Responsible for suspending a request. When the opposite request comes to the cell, it
       resumes the coroutine with the specified boolean value. This value is then returned by
       [trySuspendRequest].
       If there is a failure while trying to place a coroutine in the cell, [trySuspendRequest]
       resumes the coroutine and returns false.
     */
    private suspend fun trySuspendRequest(segment: ChannelSegment<E>, index: Int): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { segment.onInterrupt(index) }
            // Try to place the coroutine in the cell.
            if (!segment.casState(index, null, cont)) {
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

    /*
       These methods return the first segment which contains non-interrupted cells and which
       id >= the requested `destSegmentId`. Depending on the request type, either `sendSegment` or
       `receiveSegment` pointer is updated.
     */
    private fun findAndMoveForwardSend(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        while (true) {
            val destSegment = startSegment.findSegment(destSegmentId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardSend(destSegment)) {
                return destSegment
            }
        }
    }

    private fun findAndMoveForwardReceive(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        while (true) {
            val destSegment = startSegment.findSegment(destSegmentId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardReceive(destSegment)) {
                return destSegment
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

    // ###################################
    // # Validation of the channel state #
    // ###################################

    override fun checkSegmentStructureInvariants() {
        val firstSegment = getFirstSegment()

        /* Check that the `prev` link of the leftmost segment is correct. The link can be non-null, if it points
           to the segment which cells were all used by the requests and not all of them were interrupted. */
        val prev = firstSegment.getPrev()
        check(prev == null || !prev.isRemoved()) { "Channel $this: the `prev` link of the leftmost segment is not null." }

        var curSegment: ChannelSegment<E>? = firstSegment
        while (curSegment != null) {
            // Check that the segment's `prev` link is correct
            if (curSegment != firstSegment) {
                check(curSegment.getPrev() != null) { "Channel $this: `prev` link is null, but the segment $curSegment is not the leftmost one." }
                check(curSegment.getPrev()!!.getNext() == curSegment) { "Channel $this: `prev` points to the wrong segment for $curSegment." }
            }

            // Check that the removed segments are not reachable from the list.
            // The tail segment cannot be removed physically. Otherwise, uniqueness of segment id is not guaranteed.
            check(!curSegment.isRemoved() || curSegment.getNext() == null) { "Channel $this: the segment $curSegment is marked removed, but is reachable from the segment list." }

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