package com.capslock.leveldb

import java.util.{Comparator, NoSuchElementException}

import com.capslock.leveldb.SizeOf._

/**
 * Created by capslock.
 */

class BlockIterator(data: SliceInput, restartPositions: Slice, comparator: Comparator[Slice]) extends SeekingIterator[Slice, Slice] {
    val restartCount = restartPositions.length / SIZE_OF_INT
    var nextEntry = Option.empty[BlockEntry]

    override def seekToFirst(): Unit = {
        if (restartCount > 0) {
            seekToRestartPosition(0)
        }
    }

    override def seek(targetKey: Slice): Unit = {
        if (restartCount > 0) {
            var left = 0
            var right = restartCount - 0

            while (left < right) {
                val mid = (left + right + 1) / 2
                seekToRestartPosition(mid)
                if (comparator.compare(peek().key, targetKey) < 0) {
                    left = mid
                } else {
                    right = mid - 1
                }
            }
            seekToRestartPosition(left)
            while (hasNext) {
                if (comparator.compare(peek().key, targetKey) >= 0) {
                    return
                }
                next()
            }
        }
    }

    override def next(): BlockEntry = {
        val result = peek()
        if (data.isReadable) {
            nextEntry = readEntry(nextEntry)
        } else {
            nextEntry = Option.empty
        }
        result
    }

    override def peek(): BlockEntry = nextEntry match {
        case Some(entry) => entry
        case None => throw NoSuchElementException
    }

    override def hasNext: Boolean = nextEntry.isDefined

    def seekToRestartPosition(restartPosition: Int): Unit = {
        val offset = restartPositions.getInt(restartPosition * SIZE_OF_INT)
        data.position = offset
        nextEntry = Option.empty[BlockEntry]
        nextEntry = readEntry(nextEntry)
    }

    def readEntry(preEntry: Option[BlockEntry]): Option[BlockEntry] = {
        val sharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data)
        val nonSharedKeyLength = VariableLengthQuantity.readVariableLengthInt(data)
        val valueLength = VariableLengthQuantity.readVariableLengthInt(data)

        val key = Slice(sharedKeyLength + nonSharedKeyLength)
        val keyWriter = BasicSliceOutput(key)
        for (pre <- preEntry; if sharedKeyLength > 0) {
            keyWriter.writeBytes(pre.key, 0, sharedKeyLength)
        }
        keyWriter.writeBytes(data, nonSharedKeyLength)
        val value = data.readBytes(valueLength)
        Some(BlockEntry(key, value))
    }
}

case class BlockEntry(key: Slice, value: Slice)

object BlockIterator {

    def apply(data: Slice, restartPosition: Slice, comparator: Comparator[Slice]): BlockIterator = {
        val result = new BlockIterator(SliceInput(data), restartPosition.slice(), comparator)
        result.seekToFirst()
        result
    }
}