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
        val firstSegment = ChannelSegment<E>(0)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
    }

    override suspend fun send(elem: E) {
        while (true) {
            val s = sendersCounter.getAndIncrement()
            val cell = findAndMoveForwardSend(startSegment = sendSegment.value, destSegmentId = s / SEGMENT_SIZE)
                .getCell(index = (s % SEGMENT_SIZE).toInt())
            cell.setElement(elem)
            if (updateCellOnSend(s, cell)) return
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
            val r = receiversCounter.getAndIncrement()
            val cell = findAndMoveForwardReceive(startSegment = receiveSegment.value, destSegmentId = r / SEGMENT_SIZE)
                .getCell(index = (r % SEGMENT_SIZE).toInt())
            if (updateCellOnReceive(r, cell)) {
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
                    // The sender came, but the cell is empty. Clean the cell to avoid memory leaks.
                    cell.cleanElement()
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
                cell.setState(StateType.DONE)
                return true
            } else {
                cell.setState(StateType.BROKEN)
                cell.cleanElement()
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
            cont.invokeOnCancellation {
                cell.onInterrupt()
            }
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
       Returns a segment which id is equal to the given `destSegmentId`.
       If `startSegment != destSegment`, it means a new segment was created and `sendersCounter`
       now points to it. The corresponding `sendSegment` pointer should be moved forward.
     */
    private fun findAndMoveForwardSend(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        val destSegment = findSegment(startSegment, destSegmentId)
        // If `sendersCounter` moved to the next segment, update the pointer
        sendSegment.compareAndSet(startSegment, destSegment)
        return destSegment
    }

    /*
       Returns a segment which id is equal to the given `destSegmentId`.
       If `startSegment != destSegment`, it means a new segment was created and `receiversCounter`
       now points to it. The corresponding `receiveSegment` pointer should be moved forward.
     */
    private fun findAndMoveForwardReceive(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        val destSegment = findSegment(startSegment, destSegmentId)
        // If `receiversCounter` moved to the next segment, update the pointer
        receiveSegment.compareAndSet(startSegment, destSegment)
        return destSegment
    }

    private fun findSegment(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        var curSegment = startSegment
        while (curSegment.id < destSegmentId) {
            val nextSegment = ChannelSegment<E>(curSegment.id + 1)
            curSegment.next.compareAndSet(null, nextSegment)
            curSegment = curSegment.next.value!!
        }
        return curSegment
    }

    override fun checkSegmentStructureInvariants() {}
}