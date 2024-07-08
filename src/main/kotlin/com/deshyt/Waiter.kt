package com.deshyt

import java.util.concurrent.atomic.AtomicReference

class Waiter {
    val state: AtomicReference<Any> = AtomicReference(StateType.EMPTY)
    var elem: Any? = null
}

enum class StateType {
    RECEIVER, SENDER, EMPTY, DONE, BUFFERED, BROKEN, INTERRUPTED
}

