package com.deshyt.buffered

import com.deshyt.Channel
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine

class BufferedChannel<E>(capacity: Long) : Channel<E> {
    /*
      The counters show the total amount of senders and receivers ever performed. They are
      incremented in the beginning of the corresponding operation, thus acquiring a unique
      (for the operation type) cell to process.
     */
    private val sendersCounter = atomic(0L)
    private val receiversCounter = atomic(0L)

    /*
       This counter indicates the end of the channel buffer. Its value increases every time
       when a `receive` request is completed.
     */
    private val bufferEnd = atomic(capacity)

    /*
       The channel pointers indicate segments where values of `sendersCounter` and
       `receiversCounter` are currently located.
     */
    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>
    private val bufferEndSegment: AtomicRef<ChannelSegment<E>>

    init {
        val firstSegment = ChannelSegment<E>(id = 0, prevSegment = null)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
        bufferEndSegment = atomic(firstSegment.findSegment(bufferEnd.value / SEGMENT_SIZE))
    }

    override suspend fun send(elem: E) {
        while (true) {
            val startSegment = sendSegment.value
            val s = sendersCounter.getAndIncrement()
            val segment = findAndMoveForwardSend(startSegment = startSegment, destSegmentId = s / SEGMENT_SIZE)
            if (segment.id != s / SEGMENT_SIZE) {
                // The `sendSegment` pointer was updated. Skip the segment(s) with INTERRUPTED cells
                sendersCounter.compareAndSet(s + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            val index = (s % SEGMENT_SIZE).toInt()
            segment.setElement(index, elem)
            if (updateCellOnSend(s, segment, index)) return
            segment.cleanElement(index)
        }
    }

    private suspend fun updateCellOnSend(s: Long, segment: ChannelSegment<E>, index: Int): Boolean {
        while (true) {
            val state = segment.getState(index)
            val b = bufferEnd.value
            val r = receiversCounter.value
            when {
                // Empty and either the cell is in the buffer or a receiver is coming => buffer
                state == null && (s < r || s < b) || state == CellState.IN_BUFFER -> {
                    if (segment.casState(index, state, CellState.BUFFERED)) return true
                }
                // Empty, the cell is not in the buffer and no receiver is coming => suspend
                state == null && s >= b && s >= r -> {
                    val isSuspended = suspendCancellableCoroutine { cont ->
                        cont.invokeOnCancellation {
                            segment.onInterrupt(index = index, newState = CellState.INTERRUPTED_SEND)
                        }
                        if (!segment.casState(index, null, Coroutine(cont, RequestType.SEND))) {
                            // The cell is occupied by the opposite request, resume the coroutine.
                            cont.tryResumeRequest(false)
                        }
                    }
                    if (isSuspended) return true
                }
                // Suspended receiver in the cell => try to resume it
                state is Coroutine -> {
                    if (state.cont.tryResumeRequest(true)) {
                        segment.casState(index, state, CellState.DONE_RCV)
                        return true
                    } else {
                        segment.casState(index, state, CellState.POISONED)
                        return false
                    }
                }
                // The cell was poisoned by a receiver => restart the sender
                state == CellState.POISONED -> return false
                // Cancelled receiver in the cell => restart the sender
                state == CellState.INTERRUPTED_RCV -> return false
            }
        }
    }

    override suspend fun receive(): E {
        while (true) {
            val startSegment = receiveSegment.value
            val r = receiversCounter.getAndIncrement()
            val segment = findAndMoveForwardReceive(startSegment = startSegment, destSegmentId = r / SEGMENT_SIZE)
            if (segment.id != r / SEGMENT_SIZE) {
                // The `receiveSegment` pointer was updated. Skip the segment(s) with INTERRUPTED cells
                receiversCounter.compareAndSet(r + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            val index = (r % SEGMENT_SIZE).toInt()
            if (updateCellOnReceive(r, segment, index)) {
                // The element was successfully received. Return the value, then clean the cell to avoid memory leaks
                return segment.retrieveElement(index)
            }
        }
    }

    private suspend fun updateCellOnReceive(r: Long, segment: ChannelSegment<E>, index: Int): Boolean {
        while (true) {
            val state = segment.getState(index)
            val s = sendersCounter.value
            when {
                // The cell is empty and no sender is coming => suspend
                (state == null || state == CellState.IN_BUFFER) && r >= s -> {
                    val isSuspended = suspendCancellableCoroutine { cont ->
                        cont.invokeOnCancellation {
                            segment.onInterrupt(index = index, newState = CellState.INTERRUPTED_RCV)
                        }
                        if (segment.casState(index, state, Coroutine(cont, RequestType.RECEIVE))) {
                            expandBuffer()
                        } else {
                            // The cell is occupied by the opposite request. Resume the coroutine.
                            cont.tryResumeRequest(false)
                        }
                    }
                    if (isSuspended) return true
                }
                // The cell is empty but a sender is coming => poison & restart
                (state == null || state == CellState.IN_BUFFER) && r < s -> {
                    if (segment.casState(index, state, CellState.POISONED)) {
                        expandBuffer()
                        return false
                    }
                }
                // Buffered element => finish
                state == CellState.BUFFERED -> {
                    expandBuffer()
                    return true
                }
                // Interrupted sender => restart
                state == CellState.INTERRUPTED_SEND -> return false
                // The sender is in the cell waiting for a rendezvous => try to resume it
                state is Coroutine -> {
                    if (segment.casState(index, state, CellState.RESUMING_RCV)) {
                        if (state.cont.tryResumeRequest(true)) {
                            segment.casState(index, CellState.RESUMING_RCV, CellState.BUFFERED)
                        } else {
                            segment.casState(index, CellState.RESUMING_RCV, CellState.INTERRUPTED_SEND)
                        }
                    }
                }
                // `expandBuffer()` is resuming the sender => wait
                state == CellState.RESUMING_EB -> continue
            }
        }
    }

    /*
       Responsible for resuming a coroutine. The given value is the one that should be returned
       in the suspension point. If the coroutine is successfully resumed, [tryResumeRequest]
       returns true, otherwise it returns false.
     */
    @OptIn(InternalCoroutinesApi::class)
    private fun <T> CancellableContinuation<T>.tryResumeRequest(value: T): Boolean {
        val token = tryResume(value)
        if (token != null) {
            completeResume(token)
            return true
        } else {
            return false
        }
    }

    /*
       These methods return the first segment which contains non-interrupted cells and which
       id >= the requested `destSegmentId`. Depending on the request type, either `sendSegment` or
       `receiveSegment` pointer is updated.
     */
    private fun findAndMoveForwardSend(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        while (true) {
            val destSegment = startSegment.findSegment(destSegmentId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardSend(destSegment)) {
                return destSegment
            }
        }
    }

    private fun findAndMoveForwardReceive(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        while (true) {
            val destSegment = startSegment.findSegment(destSegmentId)
            // Try to update `sendSegment` and restart if the found segment is logically removed
            if (moveForwardReceive(destSegment)) {
                return destSegment
            }
        }
    }

    /*
       These methods help moving the pointers forward.
       If the pointer is being moved to the segment which is logically removed, the method
       returns false, thus forcing [findAndMoveForwardSend] (or [findAndMoveForwardReceive])
       method to restart.
     */
    private fun moveForwardSend(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = sendSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another request.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardSend].
                return false
            }
            if (sendSegment.compareAndSet(cur, to)) {
                cur.tryRemoveSegment()
                return true
            }
        }
    }

    private fun moveForwardReceive(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = receiveSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another request.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardReceive].
                return false
            }
            if (receiveSegment.compareAndSet(cur, to)) {
                cur.tryRemoveSegment()
                return true
            }
        }
    }

    /*
       This method is responsible for updating the [bufferEnd] counter. It is called after
       `receive()` successfully performs its synchronization, either retrieving the first
       element, or storing its coroutine for suspension.
     */
    private fun expandBuffer() {
        while (true) {
            val b = bufferEnd.getAndIncrement()
            if (b >= sendersCounter.value) {
                // The cell is not covered by send() request, finish
                return
            }
            // The cell stores a sender or there is an incoming one.
            val segment = findAndMoveForwardEB(startSegment = bufferEndSegment.value, destSegmentId = b / SEGMENT_SIZE)
            if (segment.id != b / SEGMENT_SIZE) {
                bufferEnd.compareAndSet(b + 1, segment.id * SEGMENT_SIZE)
                continue
            }
            if (updateCellOnExpandBuffer(segment, (b % SEGMENT_SIZE).toInt())) {
                return
            }
        }
    }

    private fun findAndMoveForwardEB(startSegment: ChannelSegment<E>, destSegmentId: Long): ChannelSegment<E> {
        while (true) {
            val destSegment = startSegment.findSegment(destSegmentId)
            // Try to update `bufferEndSegment` and restart if the found segment is logically removed
            if (moveForwardEB(destSegment)) {
                return destSegment
            }
        }
    }

    private fun moveForwardEB(to: ChannelSegment<E>): Boolean {
        while (true) {
            val cur = bufferEndSegment.value
            if (cur.id >= to.id) {
                // No need to update the pointer, it was already updated by another request.
                return true
            }
            if (to.isRemoved()) {
                // Trying to move pointer to the segment which is logically removed. Restart [findAndMoveForwardSend].
                return false
            }
            if (bufferEndSegment.compareAndSet(cur, to)) {
                return true
            }
        }
    }

    /*
       This method returns true if [expandBuffer] should finish and false if [expandBuffer] should restart.
     */
    private fun updateCellOnExpandBuffer(segment: ChannelSegment<E>, index: Int): Boolean {
        while (true) {
            when (val state = segment.getState(index)) {
                // The cell is empty => mark the cell as "in the buffer" and finish
                null -> if (segment.casState(index, null, CellState.IN_BUFFER)) return true
                is Coroutine ->
                    when (state.requestType) {
                        // A suspended sender is stored => try to resume it
                        RequestType.SEND -> {
                            if (segment.casState(index, state, CellState.RESUMING_EB)) {
                                if (state.cont.tryResumeRequest(true)) {
                                    segment.casState(index, CellState.RESUMING_EB, CellState.BUFFERED)
                                    return true
                                } else {
                                    segment.casState(index, CellState.RESUMING_EB, CellState.INTERRUPTED_SEND)
                                    return false
                                }
                            }
                        }
                        // A suspended receiver is stored => finish
                        RequestType.RECEIVE -> return true
                    }
                // The element is already buffered => finish
                CellState.BUFFERED -> return true
                // The rendezvous happened => finish, expandBuffer() was invoked before the receiver was suspended
                CellState.DONE_RCV -> return true
                // The sender was interrupted => restart
                CellState.INTERRUPTED_SEND -> return false
                // The receiver was interrupted => finish
                CellState.INTERRUPTED_RCV -> return true
                // Poisoned cell => finish, receive() is in charge
                CellState.POISONED -> return true
                // A receiver is resuming the sender => wait
                CellState.RESUMING_RCV -> continue
            }
        }
    }

    // ###################################
    // # Validation of the channel state #
    // ###################################

    override fun checkSegmentStructureInvariants() {
        val firstSegment = getFirstSegment()

        /* TODO Check that the `prev` link of the leftmost segment is correct. The link can be non-null, if it points
                to the segment which cells were all used by the requests and not all of them were interrupted. */
//        val prev = firstSegment.getPrev()
//        check(prev == null || !prev.isRemoved()) { "Channel $this: the `prev` link of the leftmost segment is not null." }

        var curSegment: ChannelSegment<E>? = firstSegment
        while (curSegment != null) {
            // Check that the segment's `prev` link is correct
            if (curSegment != firstSegment) {
                check(curSegment.getPrev() != null) { "Channel $this: `prev` link is null, but the segment $curSegment is not the leftmost one." }
                check(
                    curSegment.getPrev()!!.getNext() == curSegment
                ) { "Channel $this: `prev` points to the wrong segment for $curSegment." }
            }

            // TODO Check that the removed segments are not reachable from the list.
            // The tail segment cannot be removed physically. Otherwise, uniqueness of segment id is not guaranteed.
//            check(curSegment.isAlive() || curSegment.getNext() == null) { "Channel $this: the segment $curSegment is marked removed, but is reachable from the segment list." }

            // Check that the segment's state is correct
            curSegment.validate()

            curSegment = curSegment.getNext()
        }
    }

    /*
       This method returns the leftmost segment in the segment list.
       It handles the situation when one pointer was moved further, the other pointer remained
       the same and segments between the pointers were removed:

                         REMOVED             REMOVED
                        ┌───────┐ ────────► ┌───────┐ ────────► ┌───────┐
                        └───────┘ null◄──── └───────┘ null◄──── └───────┘
                            ▲                                       ▲
                            │                                       │
                         `receiveSegment`                        `sendSegment`

       In this case, the leftmost segment is the first alive one or the tail.
     */
    private fun getFirstSegment(): ChannelSegment<E> {
        var cur = listOf(receiveSegment.value, sendSegment.value).minBy { it.id }
        while (cur.isRemoved() && cur.getNext() != null) {
            cur = cur.getNext()!!
        }
        return cur
    }
}

internal class Coroutine(
    val cont: CancellableContinuation<Boolean>,
    val requestType: RequestType
)

internal enum class RequestType { SEND, RECEIVE }