package com.capslock.leveldb

import java.util.Comparator

import scala.collection.mutable

/**
 * Created by capslock.
 */
class Level0Iterator(val tableCache: TableCache, val files: List[FileMetaData], val comparator: Comparator[InternalKey])
    extends AbstractSeekingIterator[InternalKey, Slice] with InternalIterator {
    val inputs = files.flatMap(file => tableCache.newIterator(file))
    val priorityQueue: mutable.PriorityQueue[ComparableIterator] = mutable.PriorityQueue[ComparableIterator]()
    resetPriorityQueue()

    def resetPriorityQueue(): Unit = {
        priorityQueue.clear()
        for ((input, index) <- inputs.zipWithIndex) {
            if (input.hasNext) {
                priorityQueue.enqueue(ComparableIterator(input, comparator, index, input.nextElement))
            }
        }
    }

    override protected def seekToFirstInternal(): Unit = {
        inputs.foreach(iterator => iterator.seekToFirst())
        resetPriorityQueue()
    }

    override protected def seekInternal(targetKey: InternalKey): Unit = {
        inputs.foreach(iterator => iterator.seek(targetKey))
        resetPriorityQueue()
    }

    override protected def getNextElement(): Option[(InternalKey, Slice)] = {
        try {
            val iterator = priorityQueue.dequeue()
            val result = if (iterator.hasNext) {
                Option(iterator.next())
            } else {
                Option.empty
            }
            if (iterator.hasNext) {
                priorityQueue.enqueue(iterator)
            }
            result
        } catch {
            case _: Throwable => Option.empty
        }
    }
}

case class ComparableIterator(iterator: SeekingIterator[InternalKey, Slice], comparator: Comparator[InternalKey],
                              ordinal: Int, var nextElement: Option[(InternalKey, Slice)])
    extends Iterator[(InternalKey, Slice)] with Comparable[ComparableIterator] {
    override def hasNext: Boolean = nextElement.isDefined

    @throws(classOf[NoSuchElementException])
    override def next(): (InternalKey, Slice) = {
        if (nextElement.isDefined) {
            val result = nextElement.get
            if (iterator.hasNext) {
                nextElement = Some(iterator.next())
            } else {
                nextElement = Option.empty
            }
            result
        } else {
            throw new NoSuchElementException
        }
    }


    override def compareTo(that: ComparableIterator): Int = {
        if (nextElement.isEmpty && that.nextElement.isEmpty) {
            0
        } else if (nextElement.isEmpty) {
            -1
        } else if (that.nextElement.isEmpty) {
            1
        } else {
            var result = comparator.compare(nextElement.get._1, that.nextElement.get._1)
            if (result == 0) {
                result = ordinal.compare(that.ordinal)
            }
            result
        }
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[ComparableIterator]

    override def equals(other: Any): Boolean = other match {
        case that: ComparableIterator =>
            (that canEqual this) && ordinal == that.ordinal && nextElement == that.nextElement
        case _ => false
    }

    override def hashCode(): Int = {
        31 * ordinal + {
            if (nextElement.isDefined) {
                nextElement.get.hashCode()
            } else {
                0
            }
        }
    }
}

object Level0Iterator {
    def apply(tableCache: TableCache, files: List[FileMetaData], comparator: Comparator[InternalKey]): Level0Iterator = {
        new Level0Iterator(tableCache, files, comparator)
    }
}
