package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic


class ChannelSegment<E>(
    val id: Long
) {
    val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
    private val cells = List(SEGMENT_SIZE) { AtomicWaiter(this) }

    /*
       The counter shows how many cells are marked INTERRUPTED in the segment. If the value
       is equal to SEGMENT_SIZE, the segment should be removed.
    */
    private val interruptedCellsCounter = atomic(0)

    internal fun getCell(index: Int): AtomicWaiter<E> = cells[index]

    /*
       The segment's counter increases when the coroutine stored in one of the segment's cells
       is cancelled (see [AtomicWaiter::onInterrupt] method).
    */
    internal fun increaseInterruptedCellsCounter() { interruptedCellsCounter.incrementAndGet() }

    internal fun getInterruptedCellsCounter(): Int = interruptedCellsCounter.value

    /* This method returns true if there are cells in the segment which are not marked INTERRUPTED */
    internal fun isActive(): Boolean = interruptedCellsCounter.value < SEGMENT_SIZE
}

const val SEGMENT_SIZE = 10