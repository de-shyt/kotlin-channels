package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

/**
 * The channel is represented as a list of segments, which simulates an infinite array.
 * Each segment has its own [id], which increases from the beginning.
 *
 * The structure of the segment list is manipulated inside the methods [findSegment]
 * and [tryRemoveSegment] and cannot be changed from the outside.
 */
class ChannelSegment<E>(
    val id: Long,
    prevSegment: ChannelSegment<E>?,
    private val channel: RendezvousChannel<E> // this reference is temporary. Needed in [findPrev] method
) {
    private val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
//    private val prev: AtomicRef<ChannelSegment<E>?> = atomic(prevSegment) // TODO add `prev` link

    private val cells = List(SEGMENT_SIZE) { AtomicWaiter(this) }

    /*
       The counter shows how many cells are marked interrupted in the segment. If the value is
       equal to SEGMENT_SIZE, it means all cells were interrupted and the segment should be removed.
    */
    private val interruptedCellsCounter = atomic(0)

    // ########################################
    // # Manipulation with the segment's data #
    // ########################################

    internal fun getNext(): ChannelSegment<E>? = next.value

    private fun casNext(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = next.compareAndSet(from, to)

//    internal fun getPrev(): ChannelSegment<E>? = prev.value

//    private fun casPrev(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = prev.compareAndSet(from, to)

    internal fun getCell(index: Int): AtomicWaiter<E> = cells[index]

    /*
       These methods show the state of the segment. The segment is logically removed if all its
       cells are were interrupted. [isRemoved] method is used to check this condition.
    */
    internal fun isRemoved(): Boolean = interruptedCellsCounter() == SEGMENT_SIZE
    internal fun isActive(): Boolean = !isRemoved()

    /*
       This method is used to update `interruptedCellsCounter`. The counter increases when the
       coroutine stored in one of the segment's cells is cancelled (see [AtomicWaiter#onInterrupt]
       method).
    */
    internal fun onCellInterrupt() = increaseInterruptedCellsCounter()

    private fun increaseInterruptedCellsCounter() {
        val updatedValue = interruptedCellsCounter.incrementAndGet()
        check(updatedValue <= SEGMENT_SIZE) {
            "Segment $this: some cells were interrupted more than once (counter=$updatedValue, SEGMENT_SIZE=$SEGMENT_SIZE)"
        }
        tryRemoveSegment()
    }

    private fun interruptedCellsCounter(): Int = interruptedCellsCounter.value

    // #######################################################
    // # Manipulation with the structure of the segment list #
    // #######################################################

    /*
       This method looks for a segment with id equal to or greater than the requested id.
       If there are segments which are marked removed, they are skipped.
     */
    internal fun findSegment(destSegmentId: Long): ChannelSegment<E> {
        var curSegment = this
        while (curSegment.isRemoved() || curSegment.id < destSegmentId) {
            val nextSegment = ChannelSegment(id = curSegment.id + 1, prevSegment = curSegment, channel = channel)
            if (curSegment.casNext(null, nextSegment)) {
                // The tail was updated. Check if the old tail should be removed.
                curSegment.tryRemoveSegment()
            }
            curSegment = curSegment.getNext()!!
        }
        return curSegment
    }

    /*
       This method is responsible for removing the segment from the segment list. First, it
       checks if all cells in the segment were interrupted. Then, in case it is true, it removes
       the segment physically by updating the neighbours' `prev` and `next` links.
     */
    internal fun tryRemoveSegment() {
        if (!isRemoved()) {
            // There are non-interrupted cells, no need to remove the segment.
            return
        }
        // The tail segment cannot be removed, otherwise it is not guaranteed that each segment has a unique id.
        val next = getNext() ?: return
        val prev = findPrev()

        prev?.casNext(this, next)
//         next.casPrev(this, prev) // TODO update `cur.next.prev` link

        next.tryRemoveSegment()
        prev?.tryRemoveSegment()
    }

    /*
       This method is used for linear segment removal. It returns a segment with id smaller than
       `this.id` or null if `this` segment was already removed from the segment list.
       The method is invoked in ChannelSegment<E>#removeSegment().
     */
    private fun findPrev(): ChannelSegment<E>? {
        val firstSegment = channel.getFirstSegment()
        if (this.id <= firstSegment.id) {
            /* Method is invoked on the leftmost segment which does not have `prev` link, return null */
            return null
        }
        /* Iterate over the segment list until `cur.getNext() == this`. The condition might not be
           met if the current segment was removed by invoking `tryRemoveSegment()` from another
           segment. In this case, null is returned. */
        var cur = firstSegment
        while (cur.getNext() != this) {
            cur = cur.getNext() ?: return null
            if (cur.id > this.id) return null
        }
        return cur
    }

    override fun toString(): String = super.toString() + "(id=$id)"

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
            if (getCell(i).getState() == CellState.INTERRUPTED) interruptedCells++
        }

        // Check that the value of the segment's counter is correct
        val counter = interruptedCellsCounter()
        check(interruptedCells == counter) { "Segment $this: the segment's counter ($counter) and the amount of interrupted cells ($interruptedCells) are different." }

        // Check that the segment's state is correct
        when (interruptedCells.compareTo(SEGMENT_SIZE)) {
            -1 -> check(isActive()) { "Segment $this: there are non-interrupted cells, but the segment is logically removed." }
            0 -> {
                check(isRemoved()) { "Segment $this: all cells were interrupted, but the segment is not logically removed." }
                // Check that the state of each cell is INTERRUPTED
                for (i in 0 until SEGMENT_SIZE) {
                    check(getCell(i).getState() == CellState.INTERRUPTED) { "Segment $this: the segment is logically removed, but the cell $i is not marked INTERRUPTED." }
                }
            }
            1 -> error("Segment $this: the amount of interrupted cells ($interruptedCells) is greater than SEGMENT_SIZE (${SEGMENT_SIZE}).")
        }
    }
}

const val SEGMENT_SIZE = 2