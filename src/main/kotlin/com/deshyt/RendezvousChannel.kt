package com.deshyt

import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.resume

class RendezvousChannel<E> : Channel<E> {
    /*
      The counters show the total amount of senders and receivers ever performed.
      The counters are incremented in the beginning of the corresponding operation,
      thus, acquiring a unique (for the operation type) cell to process.
     */
    private val sendersCounter = AtomicInteger(0)
    private val receiversCounter = AtomicInteger(0)
    private val cells = ArrayList<Waiter>(1000)

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
                // The receiver should be in the cell. Making a rendezvous...
                if (cell.state.compareAndSet(StateType.RECEIVER, StateType.DONE)) {
                    return true
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
                // The cell is empty. Try placing the sender in the cell.
                if (cell.state.compareAndSet(StateType.EMPTY, StateType.SENDER)) {
                    return suspendCell(s)
                }
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
                if (cell.state.compareAndSet(StateType.SENDER, StateType.DONE)
                    || cell.state.compareAndSet(StateType.BUFFERED, StateType.DONE)) {
                    // The element was placed in the cell by the sender
                    return true
                }
                if (cell.state.compareAndSet(StateType.EMPTY, StateType.BROKEN)) {
                    // The sender came, but the cell is empty
                    return false
                }
                if (cell.state.get() == StateType.INTERRUPTED) {
                    // The cell was INTERRUPTED. Restart the receiver.
                    cell.elem = null
                    return false
                }
            } else {
                // The cell is empty. Try placing the receiver in the cell.
                if (cell.state.compareAndSet(StateType.EMPTY, StateType.RECEIVER)) {
                    return suspendCell(r)
                }
            }
        }
    }

    /*
       Responsible for suspending requests. When a request (receiver or sender) is waiting for a rendezvous,
       it suspends until the cell state is marked `StateType.DONE`.
     */
    private suspend fun suspendCell(idx: Int): Boolean {
        val cell = cells[idx]
        val requestType = cell.state.get()
        require(requestType == StateType.RECEIVER || requestType == StateType.SENDER) { "The cell state should be StateType.RECEIVER or StateType.SENDER" }

        return suspendCancellableCoroutine { continuation ->
            continuation.invokeOnCancellation {
                cell.state.compareAndSet(requestType, StateType.INTERRUPTED)
                cell.elem = null
            }
            while (true) {
                if (cell.state.get() == StateType.DONE)
                    continuation.resume(true)
            }
        }
    }
}