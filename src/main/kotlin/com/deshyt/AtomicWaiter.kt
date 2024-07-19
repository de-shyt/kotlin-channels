package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class AtomicWaiter<E>(
    private val segment: ChannelSegment<E>
) {
    private val state: AtomicRef<Any> = atomic(StateType.EMPTY)
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
    internal fun onInterrupt() {
        setState(StateType.INTERRUPTED)
        cleanElement()
        segment.increaseInterruptedCellsCounter()
    }

    override fun toString(): String {
        return "AtomicWaiter { segmentId=${segment.id}, state=${state.value}, element=${elem} }"
    }
}

enum class StateType {
    EMPTY, DONE, BUFFERED, BROKEN, INTERRUPTED
}

