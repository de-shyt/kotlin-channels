package com.deshyt

interface Channel<E> {
    suspend fun send(elem: E)
    suspend fun receive(): E
}