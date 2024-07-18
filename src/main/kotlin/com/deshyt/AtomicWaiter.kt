package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class AtomicWaiter<E> {
    private val state: AtomicRef<Any> = atomic(StateType.EMPTY)
    private var elem: E? = null

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

    internal fun setElement(value: E?) {
        elem = value
    }

    @Suppress("UNCHECKED_CAST")
    internal fun getElement() = elem as E

    internal fun retrieveElement(): E = getElement().also { cleanElement() }

    internal fun cleanElement() {
        elem = null
    }
}

enum class StateType {
    EMPTY, DONE, BUFFERED, BROKEN, INTERRUPTED
}

