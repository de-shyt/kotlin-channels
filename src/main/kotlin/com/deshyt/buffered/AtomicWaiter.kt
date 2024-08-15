package com.deshyt.buffered

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellableContinuation

class AtomicWaiter<E>(
    private val segment: ChannelSegment<E>
) {
    private val state: AtomicRef<Any> = atomic(CellState.EMPTY)
    private var elem: E? = null

    internal fun getSegmentId() = segment.id

    // #####################################
    // # Manipulation with the State Field #
    // #####################################

    internal fun setState(value: Any) {
        state.value = value
    }

    internal fun casState(from: Any, to: Any) = state.compareAndSet(from, to)

    internal fun getState() = state.value

    internal fun isInterrupted() =
        getState() == CellState.INTERRUPTED_SEND || getState() == CellState.INTERRUPTED_RCV

    // #######################################
    // # Manipulation with the Element Field #
    // #######################################

    internal fun setElement(value: E) {
        elem = value
    }

    internal fun getElement(): E? = elem

    internal fun retrieveElement(): E = getElement()!!.also { cleanElement() }

    internal fun cleanElement() {
        elem = null
    }

    /*
       This method is invoked on the cancellation of the coroutine's continuation (see
       [RendezvousChannel::trySuspendRequest] method).
       When the coroutine is cancelled, the cell's state is marked INTERRUPTED and its element
       is set to null in order to avoid memory leaks. Besides, the cell informs the
       corresponding segment about the cancellation by increasing the segment's counter.
     */
    internal fun onInterrupt(newState: CellState) {
        require(newState == CellState.INTERRUPTED_SEND || newState == CellState.INTERRUPTED_RCV)
        setState(newState)
        segment.onCellInterrupt()
        cleanElement()
    }

    // ###########################
    // # Validation of the state #
    // ###########################

    internal fun validate() {
        // Check that the state is of valid type and there are no memory leaks
        when (val state = getState()) {
            CellState.EMPTY, CellState.DONE, CellState.BROKEN, CellState.INTERRUPTED_SEND, CellState.INTERRUPTED_RCV -> {
                check(getElement() == null) { "The state is ${state}, but the element is not null in $this." }
            }
            CellState.BUFFERED -> {}
            is CancellableContinuation<*> -> {}
            else -> error("Unexpected state $state in $this.")
        }
    }
}

enum class CellState {
    EMPTY, DONE, BUFFERED, BROKEN,
    IN_BUFFER,  /* Indicates that the cell is in the buffer and the sender should not suspend */
    INTERRUPTED_SEND, INTERRUPTED_RCV,  /* Specifies the type of request which coroutine was interrupted */
    RESUMING_RCV, RESUMING_EB  /* Specifies which entity resumes the sender (a coming receiver or `expandBuffer()`) */
}

