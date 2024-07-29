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
    prevSegment: ChannelSegment<E>?,
    private val listHead: ChannelSegment<E>?
) {
    private val state: AtomicRef<SegmentState> = atomic(SegmentState.ACTIVE)
    private val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
    private val prev: AtomicRef<ChannelSegment<E>?> = atomic(prevSegment)

    private val cells = List(SEGMENT_SIZE) { AtomicWaiter(this) }

    /*
       The counter shows how many cells are marked interrupted in the segment. If the value is
       equal to SEGMENT_SIZE, it means all cells were interrupted and the segment should be removed.
    */
    private val interruptedCellsCounter = atomic(0)

    /*
       This counter monitors if the segment is used by any of the channel pointers.
       The segment can be removed, if all cells are marked interrupted and `usedByChannelPointers`
       value is equal to 0.
     */
    private val usedByChannelPointers = atomic(0)

    // ########################################
    // # Manipulation with the segment's data #
    // ########################################

    internal fun getState(): SegmentState = state.value

    internal fun getNext(): ChannelSegment<E>? = next.value

    // TODO make method private
    internal fun casNext(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = next.compareAndSet(from, to)

    internal fun getPrev(): ChannelSegment<E>? = prev.value

    private fun casPrev(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = prev.compareAndSet(from, to)

    internal fun getCell(index: Int): AtomicWaiter<E> = cells[index]

    /*
       These methods show the state of the segment. The segment is marked REMOVED if all cells
       in the segment are marked interrupted and none of the channel pointers is pointing to
       the segment.
    */
    internal fun isRemoved(): Boolean = getState() == SegmentState.REMOVED

    internal fun isInterrupted(): Boolean = interruptedCellsCounter() == SEGMENT_SIZE

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

    /*
       These methods are used to manipulate [usedByChannelPointers] counter. The counter value
       should be between 0 and 2 inclusively.
       When [increaseUsedByChannelPointersCounter] is used on the segment for the first time,
       it changes the segment's state to USED_BY_POINTER. The method returns true, if the
       counter was updated; otherwise, the segment is already marked REMOVED and the method
       returns false.
       When [decreaseUsedByChannelPointersCounter] is used, it decreases the counter and checks
       if the segment can be removed by invoking [tryRemoveSegment] method.
     */
    internal fun increaseUsedByChannelPointersCounter(): Boolean {
        state.compareAndSet(SegmentState.ACTIVE, SegmentState.USED_BY_POINTER)
        if (getState() == SegmentState.USED_BY_POINTER) {
            val updatedValue = usedByChannelPointers.incrementAndGet()
            check(updatedValue <= 2) { "Segment $this: usedByChannelPointersCounter (${updatedValue}) is greater than CHANNEL_POINTERS_AMOUNT ($CHANNEL_POINTERS_AMOUNT)" }
            return true
        }
        return false
    }

    internal fun decreaseUsedByChannelPointersCounter() {
        val updatedValue = usedByChannelPointers.decrementAndGet()
        check(updatedValue >= 0) { "Segment $this: usedByChannelPointersCounter (${updatedValue}) is less than 0" }
        tryRemoveSegment()
    }

    private fun usedByChannelPointers() = usedByChannelPointers.value

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
        while (curSegment.isInterrupted() || curSegment.id < destSegmentId) {
            val nextSegment = ChannelSegment(id = curSegment.id + 1, prevSegment = curSegment, listHead = listHead)
            curSegment.casNext(null, nextSegment)
            curSegment = curSegment.getNext()!!
        }
        return curSegment
    }

    /*
       This method is responsible for removing the segment from the segment list. First, it
       tries to logically remove the segment by marking it REMOVED. Then, in case of success,
       it removes the segment physically by updating the neighbours' `prev` and `next` links.
     */
    private fun tryRemoveSegment() {
        if (tryMarkRemoved()) {
            // All cells were interrupted and the segment is not used by the channel pointers.
            // Physically remove the segment.
            removeSegment()
        }
    }

    /*
       This method logically removes the segment from the segment list. If all cells are marked
       interrupted and the segment is not used by the channel pointers, then it is suitable for
       the removal and its state is changed to `SegmentState.REMOVED`.
     */
    private fun tryMarkRemoved(): Boolean {
        if (state.value == SegmentState.REMOVED) return true
        if (interruptedCellsCounter() == SEGMENT_SIZE && usedByChannelPointers() == 0) {
            state.compareAndSet(SegmentState.USED_BY_POINTER, SegmentState.REMOVED)
            return true
        }
        return false
    }

    /*
       This method physically removes the segment from the segment list.
     */
    private fun removeSegment() {
        // Update links of neighbouring segments
        val prev = findPrev()
        val next = getNext()
        prev.casNext(this, next)
//        next?.casPrev(this, prev)

        next?.tryRemoveSegment()
    }

    private fun findPrev(): ChannelSegment<E> {
        var cur = listHead ?: error("ChannelSegment $this was created, but list head was not set")
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
            if (getCell(i).getState() == CellState.INTERRUPTED) interruptedCells++
        }

        // Check that the value of the segment's counter is correct
        val counter = interruptedCellsCounter()
        check(interruptedCells == counter) { "Segment $this: the segment's counter ($counter) and the amount of interrupted cells ($interruptedCells) are different." }

        // Check that the segment's state is correct
        when (interruptedCells.compareTo(SEGMENT_SIZE)) {
            -1 -> {
                check(isActive() && getState() != SegmentState.REMOVED) { "Segment $this: there are non-interrupted cells, but the segment is marked removed." }
            }
            0 -> {
                if (usedByChannelPointers() > 0) {
                    check(isInterrupted() && getState() == SegmentState.USED_BY_POINTER) { "Segment $this: the segment is used by the channel pointer, but its state is not USED_BY_POINTER (state=${getState()})" }
                } else {
                    check(isRemoved() && getState() == SegmentState.REMOVED) { "Segment $this: all cells were interrupted and the segment is not used by channel pointers, but it is not marked REMOVED (state=${getState()})" }
                }
                // Check that the state of each cell is INTERRUPTED
                for (i in 0 until SEGMENT_SIZE) {
                    check(getCell(i).getState() == CellState.INTERRUPTED) { "Segment $this: the segment is marked removed, but the cell $i is not marked INTERRUPTED." }
                }
            }
            1 -> error("Segment $this: the amount of interrupted cells ($interruptedCells) is greater than SEGMENT_SIZE (${SEGMENT_SIZE}).")
        }
    }

    internal enum class SegmentState {
        ACTIVE,            // The segment is created and not used by channel pointers
        USED_BY_POINTER, // The segment is used by channel pointers
        REMOVED          // The segment is not used by channel pointers and all cells are marked interrupted
    }
}

const val SEGMENT_SIZE = 2

// Amount of pointers used in the segment list (2 for a rendezvous channel, 3 for a buffered channel)
const val CHANNEL_POINTERS_AMOUNT = 2