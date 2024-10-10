package com.deshyt.buffered

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import kotlinx.atomicfu.getAndUpdate

/**
 * The channel is represented as a list of segments, which simulates an infinite array.
 * Each segment has its own [id], which increases from the beginning.
 *
 * The structure of the segment list is manipulated inside the methods [findSegment]
 * and [tryRemoveSegment] and cannot be changed from the outside.
 */
internal class ChannelSegment<E>(
    private val channel: BufferedChannel<E>,
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
    internal val interruptedCells: Int get() = interruptedCellsCounter.value

    /**
       Represents an array of slots, the amount of slots is equal to [SEGMENT_SIZE].
       Each slot consists of 2 registers: a state and an element.
    */
    private val data = atomicArrayOfNulls<Any?>(SEGMENT_SIZE * 2)

    /**
       This bit mask indicates which channel pointers have already processed this segment. When the
       value is `0b111`, the segment can be removed physically, even though it is not marked as
       logically removed.
     */
    private val pointersMask = atomic(0b000)
    internal val mask get() = pointersMask.value

    /**
       This method is responsible for updating the segment's bit mask. In case the segment has been
       processed by all channel pointers, the method removes the segment physically by setting
       `this.next.prev` link to `null`. If the link was updated, the method returns true, otherwise
       it returns false.

       It is guaranteed that `this.next` is not null, since all channel pointers have processed the
       segment and moved forward on the segment list. Removing the processed segment is necessary,
       it helps to avoid memory leaks.
     */
    fun updateMask(channelPointer: ChannelPointerMask): Boolean {
        check(mask and channelPointer.bitMask == 0)
            { "Segment $this: the segment has already been processed by $channelPointer, trying to update the segment's mask for the second time." }
        // Update the segment's bit mask
        pointersMask.getAndUpdate { old -> old or channelPointer.bitMask }
        // Has the segment been processed by all channel pointers?
        return if (mask == ChannelPointerMask.TOTAL.bitMask) {
            // All the channel pointers have moved forward and the segment is no longer
            // needed in the segment list. Remove it physically to avoid memory leaks.
            this.next.value?.casPrev(this, null) ?: error("Segment $this: the segment has been processed by all channel pointers, but the `next` link is null.")
            true
        } else {
            // The segment is not processed by all channel pointers. Finish the operation.
            false
        }
    }

    // ######################################
    // # Manipulation with the State Fields #
    // ######################################

    internal fun getState(index: Int): Any? = data[index * 2 + 1].value

    internal fun setState(index: Int, value: Any) { data[index * 2 + 1].lazySet(value) }

    internal fun getAndSetState(index: Int, value: Any) = data[index * 2 + 1].getAndSet(value)

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

       If the cancelled request is a receiver, the method invokes [BufferedChannel.waitExpandBufferCompletion]
       to guarantee that [BufferedChannel.expandBuffer] has processed all cells before the segment
       is physically removed.
    */
    internal fun onCancellation(index: Int, isSender: Boolean) {
        val stateOnCancellation = if (isSender) CellState.INTERRUPTED_SEND else CellState.INTERRUPTED_RCV
        if (getAndSetState(index, stateOnCancellation) != stateOnCancellation) {
            // The cell is marked interrupted. Clean the cell to avoid memory leaks.
            cleanElement(index)
            // If the cancelled request is a receiver, wait until `expandBuffer()`-s
            // invoked on the cells before the current one finish.
            if (!isSender) channel.waitExpandBufferCompletion(id * SEGMENT_SIZE + index)
            // Increase the number of interrupted cells and remove the segment physically
            // in case it becomes logically removed.
            increaseInterruptedCellsCounter()
            tryRemoveSegment()
        } else {
            // The cell's state has already been set to INTERRUPTED, no further actions
            // are needed. Finish the [onCancellation] invocation.
            return
        }
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
            val nextSegment = ChannelSegment(id = curSegment.id + 1, prevSegment = curSegment, channel = channel)
            if (curSegment.casNext(null, nextSegment)) {
                // The tail was updated. Check if the old tail should be removed.
                curSegment.tryRemoveSegment()
            }
            curSegment = curSegment.getNext()!!
        }
        return curSegment
    }

    internal fun findSpecifiedOrLast(destSegmentId: Long): ChannelSegment<E> {
        // Start searching the required segment from the specified one.
        var curSegment = this
        while (curSegment.id < destSegmentId) {
            curSegment = curSegment.getNext() ?: break
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
            when (val state = getState(index)) {
                null, CellState.IN_BUFFER -> {
                    // The cell is not yet used by any request, check that it remained clean.
                    check(getElement(index) == null)
                }
                CellState.BUFFERED -> {}  // The cell stores a buffered element.
                is Coroutine, is CoroutineEB -> {}  // The cell stores a suspended request.
                CellState.DONE_RCV, CellState.POISONED -> {
                    // The cell was processed or poisoned, check that it was cleaned.
                    check(getElement(index) == null)
                }
                CellState.INTERRUPTED_RCV, CellState.INTERRUPTED_SEND -> {
                    // The cell stored an interrupted request, check that it was cleaned.
                    check(getElement(index) == null)
                    interruptedCells++
                }
                CellState.RESUMING_BY_RCV, CellState.RESUMING_BY_EB -> error("Segment $this: state is $state, but should be BUFFERED, DONE_RCV or INTERRUPTED_SEND.")
                else -> error("Unexpected state $state in $this.")
            }
        }
        // Check that the value of the segment's counter is correct
        check(interruptedCells == this.interruptedCells) { "Segment $this: the segment's counter (${this.interruptedCells}) and the amount of interrupted cells ($interruptedCells) are different." }
        // Check that, in case all cells were interrupted, the segment is logically removed.
        if (interruptedCells == SEGMENT_SIZE) {
            check(isRemoved) { "Segment $this: all cells were interrupted, but the segment is not logically removed." }
        }
    }
}

enum class CellState {
    /* The cell is in the buffer and the sender should not suspend */
    IN_BUFFER,
    /* The cell stores a buffered element. When a sender comes to a cell which is not covered
       by a receiver yet, it buffers the element and leaves the cell without suspending. */
    BUFFERED,
    /* The sender resumed the suspended receiver and a rendezvous happened */
    DONE_RCV,
    /* When a receiver comes to the cell that is already covered by a sender, but the cell is
       still empty, it breaks the cell by changing its state to `POISONED`. */
    POISONED,
    /* A coroutine was cancelled while waiting for the opposite request. */
    INTERRUPTED_SEND, INTERRUPTED_RCV,
    /* Specifies which entity resumes the sender (a coming receiver or `expandBuffer()`) */
    RESUMING_BY_RCV, RESUMING_BY_EB
}

const val SEGMENT_SIZE = 2

/**
   These masks are used in a channel segment to define whether the segment has been processed by a
   particular pointer ([BufferedChannel.sendSegment], [BufferedChannel.receiveSegment] or
   [BufferedChannel.bufferEndSegment]).
 */
enum class ChannelPointerMask(val bitMask: Int) {
    SEND_SEGMENT(0b100),     // Bitmask for sendSegment
    RECEIVE_SEGMENT(0b010),  // Bitmask for receiveSegment
    BUFFER_END_SEGMENT(0b001), // Bitmask for bufferEndSegment
    TOTAL(0b111)
}