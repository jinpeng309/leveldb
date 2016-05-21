package com.capslock.leveldb

import java.util.Comparator

import scala.collection.mutable

/**
 * Created by capslock.
 */
class MergingIterator(levelIterators: List[InternalIterator], comparator: Comparator[InternalKey])
    extends AbstractSeekingIterator[InternalKey, Slice] {
    val priorityQueue: mutable.PriorityQueue[ComparableIterator] = mutable.PriorityQueue[ComparableIterator]()

    override protected def seekToFirstInternal(): Unit = {
        levelIterators.foreach(iterator => iterator.seekToFirst())
        resetPriorityQueue()
    }

    override protected def seekInternal(targetKey: InternalKey): Unit = {
        levelIterators.foreach(iterator => iterator.seek(targetKey))
        resetPriorityQueue()
    }

    override protected def getNextElement(): Option[(InternalKey, Slice)] = {
        try {
            val comparator = priorityQueue.dequeue()
            val result = if (comparator.hasNext) Some(comparator.next()) else Option.empty
            if (comparator.hasNext) {
                priorityQueue.enqueue(comparator)
            }
            result
        } catch {
            case _: Throwable => Option.empty
        }
    }

    def resetPriorityQueue(): Unit = {
        priorityQueue.clear()
        for ((input, index) <- levelIterators.zipWithIndex) {
            if (input.hasNext) {
                val nextElement = if (input.hasNext) Some(input.next()) else Option.empty
                priorityQueue.enqueue(ComparableIterator(input, comparator, index, nextElement))
            }
        }
    }
}
