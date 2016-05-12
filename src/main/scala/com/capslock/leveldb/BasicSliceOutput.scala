package com.capslock.leveldb

/**
 * Created by capslock.
 */
class BasicSliceOutput(slice: Slice) extends SliceOutput {
    var size = 0

    override def writeShort(value: Int): Unit = {
        slice.setShort(size, value)
        size += 2
    }

    override def writeInt(value: Int): Unit = {
        slice.setInt(size, value)
        size += 4
    }

    override def writeLong(value: Long): Unit = {
        slice.setLong(size, value)
        size += 8
    }

    override def writeByte(value: Int): Unit = {
        slice.setByte(size, value)
        size += 1
    }
}
