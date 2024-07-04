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
    private val cells = Collections.unmodifiableList(ArrayList<Waiter>(1000))

    override suspend fun send(elem: E) {
        val s = sendersCounter.getAndIncrement()
        val cell = cells[s]
        cell.elem = elem
        while (true) {
            if (s < receiversCounter.get()) {
                if (cell.state.compareAndSet(CellType.BROKEN, CellType.SENDER)) {
                    // The cell was marked BROKEN by the receiver. Place the current sender in it.
                    return
                }
                // The receiver should be in the cell. Making a rendezvous...
                if (!cell.state.compareAndSet(CellType.RECEIVER, CellType.DONE)) {
                    // The receiver came, but its coroutine is not placed in the cell yet. Mark the cell BUFFERED.
                    cell.state.compareAndSet(CellType.EMPTY, CellType.BUFFERED)
                }
                return
            } else {
                // The cell is empty. Try placing the sender in the cell.
                if (!cell.state.compareAndSet(CellType.EMPTY, CellType.SENDER)) {
                    continue
                }
                // The sender is placed in the cell.
                return
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun receive(): E {
        val r = receiversCounter.getAndIncrement()
        val cell = cells[r]
        while (true) {
            if (r < sendersCounter.get()) {
                if (cell.state.compareAndSet(CellType.SENDER, CellType.DONE)) {
                    // The sender placed an element in the cell and suspended
                    break
                }
//                if (cell.state.compareAndSet(CellType.BUFFERED, CellType.DONE)) {
//                    // The sender placed an element and left without suspending
//                    break
//                }
                // The sender came, but its coroutine is not placed in the cell yet.
                cell.state.compareAndSet(CellType.EMPTY, CellType.BROKEN)
                continue
            } else {
                // The cell is empty. Try placing the receiver in the cell.
                if (!cell.state.compareAndSet(CellType.EMPTY, CellType.RECEIVER)) {
                    continue
                }
                // The receiver is placed in the cell. Waiting for the sender...
                while (true) {
                    if (cell.state.get() == CellType.DONE) {
                        break
                    }
                }
            }
        }
        val result = cell.elem as E
        cell.elem = null
        return result
    }
}