package com.capslock.leveldb

import java.util.Comparator

import com.capslock.leveldb.SizeOf._

/**
 * Created by capslock.
 */
class Block(block: Slice, comparator: Comparator[Slice]) extends SeekingIterable[Slice, Slice] {
    val restartCount = block.getInt(block.length - SizeOf.SIZE_OF_INT)
    val data = {
        if (restartCount > 0) {
            val restartOffset = block.length - SIZE_OF_INT * (1 + restartCount)
            block.slice(0, restartOffset)
        } else {
            Slice.empty
        }
    }
    val restartPositions = {
        if (restartCount > 0) {
            val restartOffset = block.length - SIZE_OF_INT * (1 + restartCount)
            block.slice(restartOffset, restartCount * SIZE_OF_INT)
        } else {
            Slice.empty
        }
    }


    override def iterator(): SeekingIterator[Slice, Slice] = BlockIterator(data, restartPositions, comparator)
}
