package com.deshyt

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.concurrent.atomic.AtomicInteger

class RendezvousChannel<E> : Channel<E> {
    /*
      The counters show the total amount of senders and receivers ever performed.
      The counters are incremented in the beginning of the corresponding operation,
      thus, acquiring a unique (for the operation type) cell to process.
     */
    private val sendersCounter = AtomicInteger(0)
    private val receiversCounter = AtomicInteger(0)
    private val cells = List(1000) { Waiter() }

    override suspend fun send(elem: E) {
        while (true) {
            val s = sendersCounter.getAndIncrement()
            cells[s].elem = elem
            if (updateCellOnSend(s)) return
        }
    }

    private suspend fun updateCellOnSend(s: Int): Boolean {
        val cell = cells[s]
        while (true) {
            if (s < receiversCounter.get()) {
                if (cell.state.get() is CancellableContinuation<*>) {
                    // The receiver is in the cell waiting for a rendezvous
                    return tryMakeRendezvous(s)
                }
                if (cell.state.compareAndSet(StateType.EMPTY, StateType.BUFFERED)) {
                    // The receiver came, but its coroutine is not placed in the cell yet. Mark the cell BUFFERED.
                    return true
                }
                if (cell.state.get() == StateType.BROKEN || cell.state.get() == StateType.INTERRUPTED) {
                    // The cell was marked BROKEN by the receiver or INTERRUPTED. Restart the sender.
                    cell.elem = null
                    return false
                }
            } else {
                // The cell is empty. Try placing the sender's coroutine in the cell.
                if (trySuspendRequest(s)) return true
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun receive(): E {
        while (true) {
            val r = receiversCounter.getAndIncrement()
            if (updateCellOnReceive(r)) {
                val elem = cells[r].elem as E
                cells[r].elem = null
                return elem
            }
        }
    }

    private suspend fun updateCellOnReceive(r: Int): Boolean {
        val cell = cells[r]
        while (true) {
            if (r < sendersCounter.get()) {
                if (cell.state.get() is CancellableContinuation<*>) {
                    // The sender is in the cell waiting for a rendezvous
                    return tryMakeRendezvous(r)
                }
                if (cell.state.compareAndSet(StateType.BUFFERED, StateType.DONE)) {
                    // The element was placed in the cell by the sender
                    return true
                }
                if (cell.state.compareAndSet(StateType.EMPTY, StateType.BROKEN)) {
                    // The sender came, but the cell is empty
                    cell.elem = null
                    return false
                }
                if (cell.state.get() == StateType.INTERRUPTED) {
                    // The cell was INTERRUPTED. Restart the receiver.
                    return false
                }
            } else {
                // The cell is empty. Try placing the receiver's coroutine in the cell.
                if (trySuspendRequest(r)) return true
            }
        }
    }

    /*
       Responsible for making a rendezvous. The method requires that there is a suspended coroutine
       of the opposite request stored in the `cells[idx]`. If the coroutine is successfully resumed,
       the cell is marked DONE; otherwise, it is marked BROKEN and resources are cleaned.
     */
    @Suppress("UNCHECKED_CAST")
    private fun tryMakeRendezvous(idx: Int): Boolean {
        val cell = cells[idx]
        require(cell.state.get() is CancellableContinuation<*>)
        { "tryMakeRendezvous(idx) is called on unoccupied cell (no suspended coroutine was found)." }
        val cont = cell.state.get() as CancellableContinuation<Boolean>
        if (cont.tryResumeRequest(true)) {
            cell.state.compareAndSet(cont, StateType.DONE)
            return true
        } else {
            cell.state.compareAndSet(cont, StateType.BROKEN)
            cell.elem = null
            return false
        }
    }

    /*
       Responsible for suspending a request. When the opposite request comes to the cell, it
       resumes the coroutine with the specified boolean value. This value is then returned by
       [trySuspendRequest].
       If there is a failure while trying to place a coroutine in the cell, [trySuspendRequest]
       attempts to resume the coroutine, then returns false.
     */
    private suspend fun trySuspendRequest(idx: Int): Boolean {
        val cell = cells[idx]
        return suspendCancellableCoroutine { cont ->
            cont.invokeOnCancellation {
                cell.state.compareAndSet(it, StateType.INTERRUPTED)
                cell.elem = null
            }
            // Try to place the coroutine in the cell.
            if (!cell.state.compareAndSet(StateType.EMPTY, cont))
                // The cell is occupied by the opposite request. Resume the coroutine.
                cont.tryResumeRequest(false)
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
}