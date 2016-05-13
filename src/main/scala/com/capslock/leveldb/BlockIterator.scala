package com.capslock.leveldb

import java.util.Comparator

import com.capslock.leveldb.BlockIterator.BlockEntry

/**
 * Created by capslock.
 */

class BlockIterator(data: SliceInput, restartPositions: Slice, comparator: Comparator[Slice]) extends SeekingIterator[Slice, Slice] {
    val restartCount = restartPositions.length / SizeOf.SIZE_OF_INT
    var nextEntry: Option[BlockEntry] = Option.empty

    override def seekToFirst: Unit = ???

    override def seek(targetKey: Slice): Unit = ???

    override def next(): BlockEntry = ???

    override def peek(): BlockEntry = ???

    override def hasNext: Boolean = ???

    def seekToRestartPosition(restartPosition: Int): Unit = {

    }
}

object BlockIterator {
    type BlockEntry = (Slice, Slice)

    def apply(data: Slice, restartPosition: Slice, comparator: Comparator[Slice]): BlockIterator = {
        val result = new BlockIterator(SliceInput(data), restartPosition.slice(), comparator)
        result.seekToFirst
        result
    }
}