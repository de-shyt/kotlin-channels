package com.deshyt

import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.*
import org.junit.*


abstract class TestBase(
    open val sequentialSpecification: Class<*>,
) {
    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .iterations(scenarios)
        .invocationsPerIteration(invokePerScenarioAmount(false))
        .threads(threadsAmount)
        .actorsBefore(actorsBeforeAmount)
        .actorsPerThread(actorsPerThreadAmount)
        .actorsAfter(actorsAfterAmount)
        .checkObstructionFreedom()
        .sequentialSpecification(sequentialSpecification)
        .check(this::class)

    @Test
    fun stressTest() = StressOptions()
        .iterations(scenarios)
        .invocationsPerIteration(invokePerScenarioAmount(true))
        .threads(threadsAmount)
        .actorsBefore(actorsBeforeAmount)
        .actorsPerThread(actorsPerThreadAmount)
        .actorsAfter(actorsAfterAmount)
        .sequentialSpecification(sequentialSpecification)
        .check(this::class)
}

// Configures the number of times the LinChecker will generate different concurrent scenarios and execute them
val scenarios = 150

// Defines the number of method calls that will be performed in each concurrent scenario.
fun invokePerScenarioAmount(isStressTest: Boolean) = if (isStressTest) 25_000 else 10_000

// Sets the number of threads that will be operating in parallel
val threadsAmount = 3

// Define the structure of each test scenario (number of operations executed before/during/after the parallel part)
val actorsBeforeAmount = 2
val actorsPerThreadAmount = 3
val actorsAfterAmount = 0