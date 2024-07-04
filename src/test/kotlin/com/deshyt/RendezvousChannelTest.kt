package com.deshyt

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.junit.Test

@Suppress("unused")
@Param(name = "elem", gen = IntGen::class, conf = "0:3")
class RendezvousChannelTest {
    private val channel = RendezvousChannel<Int>()

    @Operation
    suspend fun send(@Param(name = "elem") elem: Int) = channel.send(elem)

    @Operation
    suspend fun receive() = channel.receive()

    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .checkObstructionFreedom()
        .check(this::class)
}