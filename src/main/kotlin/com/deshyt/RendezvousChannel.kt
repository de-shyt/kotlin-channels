package com.deshyt

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
       The segment pointers indicate segments where values of `sendersCounter` and
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
                // Skip the segment(s) with INTERRUPTED cells
                sendersCounter.compareAndSet(s + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            val cell = segment.getCell(index = (s % SEGMENT_SIZE).toInt())
            cell.setElement(elem)
            if (updateCellOnSend(s, cell)) return
            cell.cleanElement()
        }
    }

    private suspend fun updateCellOnSend(s: Long, cell: AtomicWaiter<E>): Boolean {
        while (true) {
            if (s < receiversCounter.value) {
                if (cell.getState() is CancellableContinuation<*>) {
                    // The receiver is in the cell waiting for a rendezvous
                    return tryMakeRendezvous(cell)
                }
                if (cell.casState(StateType.EMPTY, StateType.BUFFERED)) {
                    // The receiver came, but its coroutine is not placed in the cell yet. Mark the cell BUFFERED.
                    return true
                }
                if (cell.getState() == StateType.BROKEN || cell.getState() == StateType.INTERRUPTED) {
                    // The cell was marked BROKEN by the receiver or INTERRUPTED. Restart the sender.
                    return false
                }
            } else {
                // The cell is empty. Try placing the sender's coroutine in the cell.
                if (trySuspendRequest(cell)) return true
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
            val cell = segment.getCell(index = (r % SEGMENT_SIZE).toInt())
            if (updateCellOnReceive(r, cell)) {
                // The element was successfully received. Return the value, then clean the cell to avoid memory leaks
                return cell.retrieveElement()
            }
        }
    }

    private suspend fun updateCellOnReceive(r: Long, cell: AtomicWaiter<E>): Boolean {
        while (true) {
            if (r < sendersCounter.value) {
                if (cell.getState() is CancellableContinuation<*>) {
                    // The sender is in the cell waiting for a rendezvous
                    return tryMakeRendezvous(cell)
                }
                if (cell.casState(StateType.BUFFERED, StateType.DONE)) {
                    // The element was placed in the cell by the sender
                    return true
                }
                if (cell.casState(StateType.EMPTY, StateType.BROKEN)) {
                    // The sender came, but the cell is empty.
                    return false
                }
                if (cell.getState() == StateType.INTERRUPTED) {
                    // The cell was INTERRUPTED. Restart the receiver.
                    return false
                }
            } else {
                // The cell is empty. Try placing the receiver's coroutine in the cell.
                if (trySuspendRequest(cell)) return true
            }
        }
    }

    /*
       Responsible for making a rendezvous. In case there is a suspended coroutine in the cell,
       if the coroutine is successfully resumed, the cell is marked DONE; otherwise, it is
       marked BROKEN and resources are cleaned.
     */
    @Suppress("UNCHECKED_CAST")
    private fun tryMakeRendezvous(cell: AtomicWaiter<E>): Boolean {
        val cont = cell.getState()
        if (cont is CancellableContinuation<*>) {
            if ((cont as CancellableContinuation<Boolean>).tryResumeRequest(true)) {
                cell.casState(cont, StateType.DONE)
                return true
            } else {
                cell.casState(cont, StateType.BROKEN)
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
       attempts to resume the coroutine, then returns false.
     */
    private suspend fun trySuspendRequest(cell: AtomicWaiter<E>): Boolean {
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation { cell.onInterrupt() }
            // Try to place the coroutine in the cell.
            if (!cell.casState(StateType.EMPTY, cont)) {
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
       id >= the given `destSegmentId`. Depending on the request type, either `sendSegment` or
       `receiveSegment` pointer is updated.
     */
    private fun findAndMoveForwardSend(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        val destSegment = startSegment.findSegment(destSegmentId)
        // If `sendersCounter` moved to the next segment or removed segments were skipped, update the `sendSegment` pointer
        sendSegment.compareAndSet(startSegment, destSegment)
        return destSegment
    }

    private fun findAndMoveForwardReceive(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        val destSegment = startSegment.findSegment(destSegmentId)
        // If `receiversCounter` moved to the next segment or removed segments were skipped, update the `receiveSegment` pointer
        receiveSegment.compareAndSet(startSegment, destSegment)
        return destSegment
    }

    // ###################################
    // # Validation of the channel state #
    // ###################################

    override fun checkSegmentStructureInvariants() {
        val firstSegment = listOf(receiveSegment.value, sendSegment.value).minBy { it.id }

        // TODO Check that the leftmost segment does not have an active `prev` link
//        check(firstSegment.getPrev() == null) {
//            "All processed segments should be unreachable from the data structure, but the `prev` link of the leftmost segment is non-null.\n" +
//            "Channel state: $this"
//        }

        var curSegment: ChannelSegment<E>? = firstSegment

        while (curSegment != null) {
            // Check that the segment's `prev` link is correct
            if (curSegment != firstSegment) {
                check(curSegment.getPrev() != null) { "Channel $this: `prev` link is null, but the segment $curSegment is not the leftmost one." }
                check(curSegment.getPrev()!!.getNext() == curSegment) { "Channel $this: `prev` points to the wrong segment for $curSegment." }
            }

            // TODO Check that the removed segments are not reachable from the list
//            check(curSegment.isActive()) { "Channel $this: the segment $curSegment is marked removed, but is reachable from the segment list." }

            // Check that the segment's state is correct
            try {
                curSegment.validate()
            } catch (e: Exception) {
                error("Channel $this:\n\t${e.message}")
            }

            curSegment = curSegment.getNext()
        }
    }
}