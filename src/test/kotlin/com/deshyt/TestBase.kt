package com.deshyt

import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.*
import org.junit.*


abstract class TestBase(
    open val sequentialSpecification: Class<*>,
    val checkObstructionFreedom: Boolean = true
) {
    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .iterations(scenarios(false))
        .invocationsPerIteration(invokePerScenarioAmount(false))
        .threads(threadsAmount)
        .actorsBefore(actorsBeforeAmount)
        .actorsPerThread(actorsPerThreadAmount)
        .actorsAfter(actorsAfterAmount)
        .checkObstructionFreedom(checkObstructionFreedom)
        .sequentialSpecification(sequentialSpecification)
        .check(this::class)

    @Test
    fun stressTest() = StressOptions()
        .iterations(scenarios(true))
        .invocationsPerIteration(invokePerScenarioAmount(true))
        .threads(threadsAmount)
        .actorsBefore(actorsBeforeAmount)
        .actorsPerThread(actorsPerThreadAmount)
        .actorsAfter(actorsAfterAmount)
        .sequentialSpecification(sequentialSpecification)
        .check(this::class)
}

// Configures the number of times the LinChecker will generate different concurrent scenarios and execute them
fun scenarios(isStressTest: Boolean) = if (isStressTest) 10 else 50

// Defines the number of method calls that will be performed in each concurrent scenario.
fun invokePerScenarioAmount(isStressTest: Boolean) = if (isStressTest) 200 else 100

// Sets the number of threads that will be operating in parallel
val threadsAmount = 3

// Define the structure of each test scenario (number of operations executed before/during/after the parallel part)
val actorsBeforeAmount = 2
val actorsPerThreadAmount = 3
val actorsAfterAmount = 0