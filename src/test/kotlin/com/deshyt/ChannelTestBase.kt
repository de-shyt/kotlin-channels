package com.deshyt

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.annotations.Validate
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import kotlin.coroutines.cancellation.CancellationException


@Param.Params(Param(name = "elem", gen = IntGen::class, conf = "1:3"),
    Param(name = "closeToken", gen = IntGen::class, conf = "1:3")
)
abstract class ChannelTestBase(
    protected val c: Channel<Int>,
    override val sequentialSpecification: Class<*>,
) : TestBase(sequentialSpecification) {

    @Operation(allowExtraSuspension = true, blocking = true)
    suspend fun send(@Param(name = "elem") elem: Int): Any = try {
        c.send(elem)
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

    @Validate
    fun validate() {
        c.checkSegmentStructureInvariants()
    }
}

private class NumberedCancellationException(number: Int) : CancellationException() {
    val testResult = "Closed($number)"
}
