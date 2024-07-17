package com.deshyt

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic


class ChannelSegment<E>(
    val id: Long
) {
    val prev: AtomicRef<ChannelSegment<E>?> = atomic(null)
    val next: AtomicRef<ChannelSegment<E>?> = atomic(null)
    private val cells = List(SEGMENT_SIZE) { AtomicWaiter<E>() }

    fun getCell(idx: Long): AtomicWaiter<E> {
        require(idx / SEGMENT_SIZE == id)
            { "Trying to retrieve cell from the wrong segment (segment id: $id, given id: ${idx / SEGMENT_SIZE})" }
        return cells[(idx % SEGMENT_SIZE).toInt()]
    }
}

const val SEGMENT_SIZE = 10