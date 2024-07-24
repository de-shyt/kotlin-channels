package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

/**
 * The channel is represented as a list of segments, which simulates an infinite array.
 * Each segment has its own [id], which increases from the beginning.
 *
 * The structure of the segment list is manipulated inside the methods [findSegment]
 * and [removeSegment] and cannot be changed from the outside.
 */
class ChannelSegment<E>(
    val id: Long,
    prevSegment: ChannelSegment<E>?
) {
    private val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
    private val prev: AtomicRef<ChannelSegment<E>?> = atomic(prevSegment)

    private val cells = List(SEGMENT_SIZE) { AtomicWaiter(this) }

    /*
       The counter shows how many cells are marked INTERRUPTED in the segment. If the value is
       equal to SEGMENT_SIZE, it means all cells were interrupted and the segment should be removed.
    */
    private val interruptedCellsCounter = atomic(0)

    // ########################################
    // # Manipulation with the segment's data #
    // ########################################

    internal fun getNext(): ChannelSegment<E>? = next.value

    // TODO make method private
    internal fun casNext(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = next.compareAndSet(from, to)

    internal fun getPrev(): ChannelSegment<E>? = prev.value

    private fun casPrev(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = prev.compareAndSet(from, to)

    internal fun getCell(index: Int): AtomicWaiter<E> = cells[index]

    private fun invalidateLinks() {
        next.value = null
        prev.value = null
    }

    /*
       These methods show the state of the segment. The segment is marked removed if all cells
       in the segment are marked INTERRUPTED.
    */
    internal fun isRemoved(): Boolean = interruptedCellsCounter.value == SEGMENT_SIZE
    internal fun isActive(): Boolean = !isRemoved()

    internal fun getInterruptedCellsCounter(): Int = interruptedCellsCounter.value

    /*
       The segment's counter increases when the coroutine stored in one of the segment's cells
       is cancelled (see [AtomicWaiter::onInterrupt] method). If all cells are marked INTERRUPTED,
       the segment is removed.
    */
    internal fun increaseInterruptedCellsCounter() {
        val updatedValue = interruptedCellsCounter.incrementAndGet()
        check(updatedValue <= SEGMENT_SIZE) {
            "Segment $this: some cells were interrupted more than once (counter=$updatedValue, SEGMENT_SIZE=$SEGMENT_SIZE)"
        }
        if (isRemoved()) {
            // All cells are marked INTERRUPTED, physically remove the segment
            removeSegment()
        }
    }

    // #######################################################
    // # Manipulation with the structure of the segment list #
    // #######################################################

    /*
       This method looks for a segment with the specified id. If there are segments which are
       marked removed, they are skipped. The search finishes when it finds the first segment
       with non-interrupted cells and id >= destSegmentId.
     */
    internal fun findSegment(destSegmentId: Long): ChannelSegment<E> {
        var curSegment = this
        while (curSegment.isRemoved() || curSegment.id < destSegmentId) {
            val nextSegment = ChannelSegment(id = curSegment.id + 1, prevSegment = curSegment)
            curSegment.casNext(null, nextSegment)
            curSegment = curSegment.getNext()!!
        }
        return curSegment
    }

    /*
       This method removes the segment from the segment list.
     */
    private fun removeSegment() {
        // Update links of neighbour segments
        val prev = findPrev()
        val next = getNext()
        prev.casNext(this, next)
//        next?.casPrev(this, prev)

        if (next?.isRemoved() == true) {
            next.removeSegment()
        }

        // Invalidate links in the removed segment
        invalidateLinks()
    }

    private fun findPrev(): ChannelSegment<E> {
        var cur = HEAD
        while (cur.getNext() != this) {
            cur = cur.getNext() ?: error("cur.id=${cur.id}, prev.id=${cur.getPrev()?.id ?: "null"}, next.id=${cur.getNext()?.id ?: "null"}\n" +
            "this.id=${this.id}")

        }
        return cur as ChannelSegment<E>
    }

    // #####################################
    // # Validation of the segment's state #
    // #####################################

    internal fun validate() {
        var interruptedCells = 0

        for (i in 0 until SEGMENT_SIZE) {
            // Check that the cell is bounded with the right segment
            check(getCell(i).getSegmentId() == id) { "Segment $this: the cell $i is bounded with the wrong segment." }

            // Check that there are no memory leaks
            try {
                getCell(i).validate()
            } catch (e: Exception) {
                error("Segment $this: ${e.message}")
            }

            // Count the actual amount of interrupted cells
            if (getCell(i).getState() == StateType.INTERRUPTED) interruptedCells++
        }

        // Check that the value of the segment's counter is correct
        val counter = getInterruptedCellsCounter()
        check(interruptedCells == counter) { "Segment $this: the segment's counter ($counter) and the amount of interrupted cells ($interruptedCells) are different." }

        // Check that the segment's state is correct
        when (interruptedCells.compareTo(SEGMENT_SIZE)) {
            -1 -> check(isActive()) { "Segment $this: there are non-interrupted cells, but the segment is marked removed." }
            0 -> {
                check(isRemoved()) { "Segment $this: all cells were interrupted, but the segment is not marked removed." }
                for (i in 0 until SEGMENT_SIZE) {
                    check(getCell(i).getState() == StateType.INTERRUPTED) { "Segment $this: the segment is marked removed, but the cell $i is not marked INTERRUPTED." }
                }
            }
            1 -> error("Segment $this: the amount of interrupted cells ($interruptedCells) is greater than SEGMENT_SIZE (${SEGMENT_SIZE}).")
        }
    }
}

const val SEGMENT_SIZE = 2