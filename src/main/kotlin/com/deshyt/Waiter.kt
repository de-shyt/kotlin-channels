package com.deshyt

import java.util.concurrent.atomic.AtomicReference

class Waiter {
    val state: AtomicReference<CellType> = AtomicReference(CellType.EMPTY)
    var elem: Any? = null
}

enum class CellType {
    RECEIVER, SENDER, EMPTY, DONE, BUFFERED, BROKEN, INTERRUPTED
}

