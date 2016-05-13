package com.capslock.leveldb.comparator

import com.capslock.leveldb.Slice

/**
 * Created by capslock.
 */
class BytewiseComparator extends UserComparator {
    override def name: String = "leveldb.BytewiseComparator"

    override def findShortSuccessor(key: Slice): Slice = {
        0.to(key.length - 1).find(index => key.getUnsignedByte(index) != 0xff).flatMap(index => {
            val value = key.getUnsignedByte(index)
            val result = key.copySlice(0, index + 1)
            result.setByte(index, value + 1)
            Some(result)
        }).getOrElse(key)
    }

    override def findShortestSeparator(start: Slice, limit: Slice): Slice = {
        import Slice.calculateCommonBytes
        val commonBytes = calculateCommonBytes(Option(start), Option(limit))
        if (commonBytes < Math.min(start.length, limit.length)) {
            val firstDiffByte = start.getUnsignedByte(commonBytes)
            if (firstDiffByte < 0xff && firstDiffByte + 1 < limit.getUnsignedByte(commonBytes)) {
                val result = start.copySlice(0, commonBytes + 1)
                result.setByte(commonBytes, firstDiffByte + 1)
                return result
            }
        }
        start
    }

    override def compare(sliceA: Slice, sliceB: Slice): Int = sliceA compareTo sliceB
}

object BytewiseComparator {
    def apply(): BytewiseComparator = {
        new BytewiseComparator()
    }
}
