@file:Suppress("unused", "MemberVisibilityCanBePrivate")

package com.deshyt

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*

class RendezvousChannelLincheckTest : ChannelLincheckTestBase(
    c = RendezvousChannel(),
    sequentialSpecification = SequentialRendezvousChannel::class.java,
    obstructionFree = false
)

class SequentialRendezvousChannel : SequentialIntChannelBase(0)

@Param.Params(
    Param(name = "value", gen = IntGen::class, conf = "1:9"),
    Param(name = "closeToken", gen = IntGen::class, conf = "1:9")
)
abstract class ChannelLincheckTestBase(
    protected val c: Channel<Int>,
    private val sequentialSpecification: Class<*>,
    private val obstructionFree: Boolean = true
) : AbstractLincheckTest() {

    @Operation(allowExtraSuspension = true, blocking = true)
    suspend fun send(@Param(name = "value") value: Int): Any = try {
        c.send(value)
    } catch (e: NumberedCancellationException) {
        e.testResult
    }

    @Operation(allowExtraSuspension = true, blocking = true)
    suspend fun receive(): Any = try {
        c.receive()
    } catch (e: NumberedCancellationException) {
        e.testResult
    }

//    @Operation(causesBlocking = true, blocking = true)
//    fun close(@Param(name = "closeToken") token: Int): Boolean = c.close(NumberedCancellationException(token))
//
//    @Operation(causesBlocking = true, blocking = true)
//    fun cancel(@Param(name = "closeToken") token: Int) = c.cancel(NumberedCancellationException(token))

//    // @Operation TODO non-linearizable in BufferedChannel
//    open fun isClosedForReceive() = c.isClosedForReceive
//
//    @Operation(blocking = true)
//    fun isClosedForSend() = c.isClosedForSend

//    // @Operation TODO non-linearizable in BufferedChannel
//    open fun isEmpty() = c.isEmpty

//    @StateRepresentation
//    fun state() = (c as? BufferedChannel<*>)?.toStringDebug() ?: c.toString()
//
//    @Validate
//    fun validate() {
//        (c as? BufferedChannel<*>)?.checkSegmentStructureInvariants()
//    }

    override fun <O : Options<O, *>> O.customize(isStressTest: Boolean) =
        actorsBefore(0).sequentialSpecification(sequentialSpecification)

    override fun ModelCheckingOptions.customize(isStressTest: Boolean) =
        checkObstructionFreedom(obstructionFree)
}

private class NumberedCancellationException(number: Int) : CancellationException() {
    val testResult = "Closed($number)"
}


abstract class SequentialIntChannelBase(private val capacity: Int) {
    private val senders   = ArrayList<Pair<CancellableContinuation<Any>, Int>>()
    private val receivers = ArrayList<CancellableContinuation<Any>>()
    private val buffer = ArrayList<Int>()
    private var closedMessage: String? = null

    suspend fun send(x: Int): Any = when (val offerRes = trySend(x)) {
        true -> Unit
        false -> suspendCancellableCoroutine { cont ->
            senders.add(cont to x)
        }
        else -> offerRes
    }

    fun trySend(element: Int): Any {
        if (closedMessage !== null) return closedMessage!!
        if (capacity == CONFLATED) {
            if (resumeFirstReceiver(element)) return true
            buffer.clear()
            buffer.add(element)
            return true
        }
        if (resumeFirstReceiver(element)) return true
        if (buffer.size < capacity) {
            buffer.add(element)
            return true
        }
        return false
    }

    private fun resumeFirstReceiver(element: Int): Boolean {
        while (receivers.isNotEmpty()) {
            val r = receivers.removeAt(0)
            if (r.resume(element)) return true
        }
        return false
    }

    suspend fun receive(): Any = tryReceive() ?: suspendCancellableCoroutine { cont ->
        receivers.add(cont)
    }

    suspend fun receiveCatching() = receive()

    fun tryReceive(): Any? {
        if (buffer.isNotEmpty()) {
            val el = buffer.removeAt(0)
            resumeFirstSender().also {
                if (it !== null) buffer.add(it)
            }
            return el
        }
        resumeFirstSender()?.also { return it }
        if (closedMessage !== null) return closedMessage
        return null
    }

    private fun resumeFirstSender(): Int? {
        while (senders.isNotEmpty()) {
            val (s, el) = senders.removeAt(0)
            if (s.resume(Unit)) return el
        }
        return null
    }

    suspend fun sendViaSelect(element: Int) = send(element)
    suspend fun receiveViaSelect() = receive()

    fun close(token: Int): Boolean {
        if (closedMessage !== null) return false
        closedMessage = "Closed($token)"
        for (r in receivers) r.resume(closedMessage!!)
        receivers.clear()
        return true
    }

    fun cancel(token: Int) {
        close(token)
        for ((s, _) in senders) s.resume(closedMessage!!)
        senders.clear()
        buffer.clear()
    }

    fun isClosedForSend(): Boolean = closedMessage !== null
    fun isClosedForReceive(): Boolean = isClosedForSend() && buffer.isEmpty() && senders.isEmpty()

    fun isEmpty(): Boolean {
        if (closedMessage !== null) return false
        return buffer.isEmpty() && senders.isEmpty()
    }
}

@OptIn(InternalCoroutinesApi::class)
private fun <T> CancellableContinuation<T>.resume(res: T): Boolean {
    val token = tryResume(res) ?: return false
    completeResume(token)
    return true
}
