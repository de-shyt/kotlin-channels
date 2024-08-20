@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.deshyt

import com.deshyt.rendezvous.RendezvousChannel
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.*


class RendezvousChannelTest : ChannelTestBase(
    c = RendezvousChannel(),
    sequentialSpecification = SequentialRendezvousChannel::class.java,
)

// Sequential specification for a rendezvous channel
class SequentialRendezvousChannel {
    private val senders   = ArrayList<Pair<CancellableContinuation<Unit>, Int>>()
    private val receivers = ArrayList<CancellableContinuation<Int>>()

    suspend fun send(x: Int) {
        if (resumeFirstReceiver(x)) return
        suspendCancellableCoroutine { cont ->
            senders.add(Pair(cont, x))
        }
    }

    private fun resumeFirstReceiver(element: Int): Boolean {
        while (receivers.isNotEmpty()) {
            val r = receivers.removeAt(0)
            if (r.resume(element)) return true
        }
        return false
    }

    suspend fun receive(): Int {
        val elem = resumeFirstSender()
        if (elem != null) return elem
        return suspendCancellableCoroutine { cont ->
            receivers.add(cont)
        }
    }

    private fun resumeFirstSender(): Int? {
        while (senders.isNotEmpty()) {
            val (sender, elem) = senders.removeAt(0)
            if (sender.resume(Unit)) return elem
        }
        return null
    }
}

@OptIn(InternalCoroutinesApi::class)
private fun <T> CancellableContinuation<T>.resume(res: T): Boolean {
    val token = tryResume(res) ?: return false
    completeResume(token)
    return true
}