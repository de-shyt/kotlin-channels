@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.deshyt

class RendezvousChannelTest : ChannelTestBase(
    c = RendezvousChannel(),
    sequentialSpecification = SequentialRendezvousChannel::class.java,
    obstructionFree = false
)

// Sequential specification for a rendezvous channel
class SequentialRendezvousChannel {
    private val channel = RendezvousChannel<Int>()

    suspend fun send(x: Int) = channel.send(x)
    suspend fun receive() = channel.receive()
}
