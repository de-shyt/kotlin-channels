package com.deshyt.rendezvous

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import kotlinx.coroutines.CancellableContinuation

/**
   The channel is represented as a list of segments, which simulates an infinite array.
   Each segment has its own [id], which increases from the beginning.

   The structure of the segment list is manipulated inside the methods [findSegment]
   and [tryRemoveSegment] and cannot be changed from the outside.
 */
internal class ChannelSegment<E>(
    val id: Long,
    prevSegment: ChannelSegment<E>?,
) {
    private val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
    private val prev: AtomicRef<ChannelSegment<E>?> = atomic(prevSegment)

    /**
       This counter shows how many cells are marked interrupted in the segment. If the value
       is equal to [SEGMENT_SIZE], it means all cells were interrupted and the segment should be
       physically removed.
    */
    private val interruptedCellsCounter = atomic(0)
    private val interruptedCells: Int get() = interruptedCellsCounter.value

    /**
       Represents an array of slots, the amount of slots is equal to [SEGMENT_SIZE].
       Each slot consists of 2 registers: a state and an element.
    */
    private val data = atomicArrayOfNulls<Any?>(SEGMENT_SIZE * 2)

    // ######################################
    // # Manipulation with the State Fields #
    // ######################################

    internal fun setState(index: Int, value: Any) { data[index * 2 + 1].lazySet(value) }

    internal fun getState(index: Int): Any? = data[index * 2 + 1].value

    internal fun casState(index: Int, from: Any?, to: Any) = data[index * 2 + 1].compareAndSet(from, to)

    // ########################################
    // # Manipulation with the Element Fields #
    // ########################################

    internal fun setElement(index: Int, value: E) { data[index * 2].value = value }

    @Suppress("UNCHECKED_CAST")
    internal fun getElement(index: Int): E? = data[index * 2].value as E?

    internal fun retrieveElement(index: Int): E = getElement(index)!!.also { cleanElement(index) }

    internal fun cleanElement(index: Int) { data[index * 2].lazySet(null) }

    // ###################################################
    // # Manipulation with the segment's neighbour links #
    // ###################################################

    internal fun getNext(): ChannelSegment<E>? = next.value

    private fun casNext(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = next.compareAndSet(from, to)

    internal fun getPrev(): ChannelSegment<E>? = prev.value

    private fun casPrev(from: ChannelSegment<E>?, to: ChannelSegment<E>?) = prev.compareAndSet(from, to)

    // ########################
    // # Cancellation Support #
    // ########################

    /**
       This method is invoked on the cancellation of the coroutine's continuation. When the
       coroutine is cancelled, the cell's state is marked interrupted, its element is set to `null`
       in order to avoid memory leaks and the segment's counter of interrupted cells is increased.
    */
    internal fun onCancellation(index: Int) {
        setState(index, CellState.INTERRUPTED)
        cleanElement(index)
        increaseInterruptedCellsCounter()
        tryRemoveSegment()
    }

    private fun increaseInterruptedCellsCounter() {
        val updatedValue = interruptedCellsCounter.incrementAndGet()
        check(updatedValue <= SEGMENT_SIZE) {
            "Segment $this: some cells were interrupted more than once (counter=$updatedValue, SEGMENT_SIZE=$SEGMENT_SIZE)."
        }
    }

    // ###################################################
    // # Manipulation with the structure of segment list #
    // ###################################################

    /**
       This value shows whether the segment is logically removed. It returns true if all cells
       in the segment were marked interrupted.
    */
    internal val isRemoved: Boolean get() = interruptedCells == SEGMENT_SIZE

    /**
       This method looks for a segment with id equal to or greater than the requested [destSegmentId].
       If there are segments which are logically removed, they are skipped.
     */
    internal fun findSegment(destSegmentId: Long): ChannelSegment<E> {
        var curSegment = this
        while (curSegment.isRemoved || curSegment.id < destSegmentId) {
            val nextSegment = ChannelSegment(id = curSegment.id + 1, prevSegment = curSegment)
            if (curSegment.casNext(null, nextSegment)) {
                // The tail was updated. Check if the old tail should be removed.
                curSegment.tryRemoveSegment()
            }
            curSegment = curSegment.getNext()!!
        }
        return curSegment
    }

    /**
       This method is responsible for removing the segment from the segment list. First, it
       checks if all cells in the segment were interrupted. Then, in case it is true, it removes
       the segment physically by updating the neighbours' [prev] and [next] links.
     */
    internal fun tryRemoveSegment() {
        if (!isRemoved) {
            // There are non-interrupted cells, no need to remove the segment.
            return
        }
        if (getNext() == null) {
            // The tail segment cannot be removed, otherwise it is not guaranteed that each segment has a unique id.
            return
        }
        // Find the closest non-removed segments on the left and on the right
        val prev = aliveSegmentLeft()
        val next = aliveSegmentRight()

        // Update the links
        prev?.casNext(this, next)
        next.casPrev(this, prev)

        next.tryRemoveSegment()
        prev?.tryRemoveSegment()
    }

    /**
       This method is used to find the closest alive segment on the left from `this` segment.
       If such a segment does not exist, `null` is returned.
     */
    private fun aliveSegmentLeft(): ChannelSegment<E>? {
        var cur = getPrev()
        while (cur != null && cur.isRemoved) {
            cur = cur.getPrev()
        }
        return cur
    }

    /**
       This method is used to find the closest alive segment on the right from `this` segment.
       The tail segment is returned, if the end of the segment list is reached.
     */
    private fun aliveSegmentRight(): ChannelSegment<E> {
        var cur = getNext()
        while (cur!!.isRemoved && cur.getNext() != null) {
            cur = cur.getNext()
        }
        return cur
    }

    // #####################################
    // # Validation of the segment's state #
    // #####################################

    override fun toString(): String = "ChannelSegment(id=$id)"

    internal fun validate() {
        var interruptedCells = 0

        for (index in 0 until SEGMENT_SIZE) {
            // Check that there are no memory leaks
            cellValidate(index)
            // Count the actual amount of interrupted cells
            if (getState(index) == CellState.INTERRUPTED) interruptedCells++
        }

        // Check that the value of the segment's counter is correct
        check(interruptedCells == this.interruptedCells) { "Segment $this: the segment's counter (${this.interruptedCells}) and the amount of interrupted cells ($interruptedCells) are different." }

        // Check that the segment's state is correct
        when (interruptedCells.compareTo(SEGMENT_SIZE)) {
            -1 -> check(!isRemoved) { "Segment $this: there are non-interrupted cells, but the segment is logically removed." }
            0 -> {
                check(isRemoved) { "Segment $this: all cells were interrupted, but the segment is not logically removed." }
                // Check that the state of each cell is INTERRUPTED
                for (i in 0 until SEGMENT_SIZE) {
                    check(getState(i) == CellState.INTERRUPTED) { "Segment $this: the segment is logically removed, but the cell $i is not marked INTERRUPTED." }
                }
            }
            1 -> error("Segment $this: the amount of interrupted cells ($interruptedCells) is greater than SEGMENT_SIZE ($SEGMENT_SIZE).")
        }
    }

    private fun cellValidate(index: Int) {
        when(val state = getState(index)) {
            null, CellState.DONE, CellState.POISONED, CellState.INTERRUPTED -> {
                check(getElement(index) == null) { "Segment $this: state is ${state}, but the element is not null in cell $index." }
            }
            CellState.BUFFERED -> {}
            is CancellableContinuation<*> -> {}
            else -> error("Unexpected state $state in $this.")
        }
    }
}

enum class CellState {
    /* The element was successfully transferred to a receiver. */
    DONE,
    /* The cell stores a buffered element. When a sender comes to a cell which is not covered
       by a receiver yet, it buffers the element and leaves the cell without suspending. */
    BUFFERED,
    /* When a receiver comes to the cell that is already covered by a sender, but the cell is
       still in `EMPTY` state, it breaks the cell by changing its state to `POISONED`. */
    POISONED,
    /* A coroutine was cancelled while waiting for the opposite request. */
    INTERRUPTED
}

const val SEGMENT_SIZE = 2