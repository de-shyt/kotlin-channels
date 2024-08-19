@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.deshyt

import com.deshyt.buffered.BufferedChannel
import com.deshyt.rendezvous.RendezvousChannel
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.collections.ArrayList


class RendezvousChannelTest : ChannelTestBase(
    c = RendezvousChannel(),
    sequentialSpecification = SequentialRendezvousChannel::class.java,
    obstructionFree = true
)

class Buffered2ChannelTest : ChannelTestBase(
    c = BufferedChannel(2),
    sequentialSpecification = SequentialBuffered2Channel::class.java,
    obstructionFree = true
)

class Buffered1ChannelTest : ChannelTestBase(
    c = BufferedChannel(1),
    sequentialSpecification = SequentialBuffered1Channel::class.java,
    obstructionFree = true
)

// Sequential specification for a rendezvous channel
class SequentialRendezvousChannel {
    private val senders = ArrayList<Pair<CancellableContinuation<Unit>, Int>>()
    private val receivers = ArrayList<CancellableContinuation<Int>>()

    suspend fun send(x: Int) {
        if (resumeFirstReceiver(x)) return
        suspendCancellableCoroutine { cont ->
            senders.add(Pair(cont, x))
        }
    }

    private fun resumeFirstReceiver(element: Int): Boolean {
        while (receivers.isNotEmpty()) {
            val r = receivers.removeFirst()
            if (r.resume(element)) return true
        }
        return false
    }

    suspend fun receive(): Int {
        return resumeFirstSender()
            ?: suspendCancellableCoroutine { cont -> receivers.add(cont) }
    }

    private fun resumeFirstSender(): Int? {
        while (senders.isNotEmpty()) {
            val (sender, elem) = senders.removeAt(0)
            if (sender.resume(Unit)) return elem
        }
        return null
    }
}

// Sequential specification for a buffered channel
open class SequentialBufferedChannel(
    private val capacity: Long
) {
    private val senders = ArrayList<Pair<CancellableContinuation<Unit>, Int>>()
    private val bufferedSenders = ArrayList<Int>()
    private val receivers = ArrayList<CancellableContinuation<Int>>()

    suspend fun send(x: Int) {
        if (resumeFirstReceiver(x)) return
        if (tryBufferElem(x)) return
        suspendCancellableCoroutine { cont -> senders.add(Pair(cont, x)) }
    }

    private fun tryBufferElem(x: Int): Boolean {
        if (bufferedSenders.size < capacity) bufferedSenders.add(x).also { return true }
        return false
    }

    private fun resumeFirstReceiver(element: Int): Boolean {
        while (receivers.isNotEmpty()) {
            val r = receivers.removeFirst()
            if (r.resume(element)) return true
        }
        return false
    }

    suspend fun receive(): Int {
        return getBufferedElem()
            ?: resumeFirstSender()
            ?: suspendCancellableCoroutine { cont -> receivers.add(cont) }
    }

    private fun getBufferedElem(): Int? {
        val elem = bufferedSenders.removeFirstOrNull()?.also {
            // The element is retrieved from the buffer, resume one sender and add its element to the buffer
            resumeFirstSender()?.also { bufferedSenders.add(it) }
        }
        return elem
    }

    private fun resumeFirstSender(): Int? {
        while (senders.isNotEmpty()) {
            val (sender, elem) = senders.removeFirst()
            if (sender.resume(Unit)) return elem
        }
        return null
    }
}

class SequentialBuffered1Channel : SequentialBufferedChannel(1)
class SequentialBuffered2Channel : SequentialBufferedChannel(2)

@OptIn(InternalCoroutinesApi::class)
private fun <T> CancellableContinuation<T>.resume(res: T): Boolean {
    val token = tryResume(res) ?: return false
    completeResume(token)
    return true
}