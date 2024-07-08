package com.deshyt

import java.util.*
import java.util.concurrent.atomic.AtomicInteger

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

    private fun updateCellOnSend(s: Int): Boolean {
        val cell = cells[s]
        while (true) {
            if (s < receiversCounter.get()) {
                // The receiver should be in the cell. Making a rendezvous...
                if (cell.state.compareAndSet(CellType.RECEIVER, CellType.DONE)) {
                    return true
                }
                if (cell.state.compareAndSet(CellType.EMPTY, CellType.BUFFERED)) {
                    // The receiver came, but its coroutine is not placed in the cell yet. Mark the cell BUFFERED.
                    return true
                }
                if (cell.state.get() == CellType.BROKEN || cell.state.get() == CellType.INTERRUPTED) {
                    // The cell was marked BROKEN by the receiver or INTERRUPTED. Restart the sender.
                    cell.elem = null
                    return false
                }
            } else {
                // The cell is empty. Try placing the sender in the cell.
                if (cell.state.compareAndSet(CellType.EMPTY, CellType.SENDER)) {
                    return true
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

    private fun updateCellOnReceive(r: Int): Boolean {
        val cell = cells[r]
        while (true) {
            if (r < sendersCounter.get()) {
                if (cell.state.compareAndSet(CellType.SENDER, CellType.DONE)
                    || cell.state.compareAndSet(CellType.BUFFERED, CellType.DONE)) {
                    // The element was placed in the cell by the sender
                    return true
                }
                if (cell.state.compareAndSet(CellType.EMPTY, CellType.BROKEN)) {
                    // The sender came, but the cell is empty
                    return false
                }
                if (cell.state.get() == CellType.INTERRUPTED) {
                    // The cell was INTERRUPTED. Restart the receiver.
                    cell.elem = null
                    return false
                }
            } else {
                // The cell is empty. Try placing the receiver in the cell.
                if (cell.state.compareAndSet(CellType.EMPTY, CellType.RECEIVER)) {
                    return true
                }
            }
        }
    }
}