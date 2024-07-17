package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class AtomicWaiter<E> {
    val state: AtomicRef<Any> = atomic(StateType.EMPTY)
    var elem: E? = null
}

enum class StateType {
    EMPTY, DONE, BUFFERED, BROKEN, INTERRUPTED
}

