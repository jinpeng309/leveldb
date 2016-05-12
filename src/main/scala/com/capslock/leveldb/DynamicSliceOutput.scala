package com.capslock.leveldb

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * Created by capslock.
 */
class DynamicSliceOutput(estimatedSize: Int) extends SliceOutput {
    var size = 0
    var sliceData = Slice(estimatedSize)

    override def reset: Unit = size = 0

    override def writeBytes(source: Slice): Unit = writeBytes(source, 0, source.length)

    override def writeBytes(source: Slice, length: Int): Unit = writeBytes(source, 0, length)

    override def writeBytes(source: Slice, index: Int, length: Int): Unit = {
        sliceData = ensureSize(sliceData, size + length)
        sliceData.setBytes(size, source, index, length)
        size += length
    }

    override def writeBytes(source: Array[Byte]): Unit = writeBytes(source, 0, source.length)

    override def writeBytes(source: Array[Byte], sourceIndex: Int, length: Int): Unit = {
        sliceData = ensureSize(sliceData, size + length)
        sliceData.setBytes(size, source, sourceIndex, length)
        size += length
    }

    override def writeBytes(source: ByteBuffer): Unit = {
        val length = source.remaining
        sliceData = ensureSize(sliceData, size + length)
        sliceData.setBytes(size, source)
        size += length
    }

    override def writeBytes(source: InputStream, length: Int): Unit = {
        sliceData = ensureSize(sliceData, size + length)
        sliceData.setBytes(size, source, length)
        size += length
    }

    override def writeBytes(source: FileChannel, position: Int, length: Int): Unit = {
        sliceData = ensureSize(sliceData, size + length)
        sliceData.setBytes(size, source, position, length)
        size += length
    }

    override def writableBytes: Int = sliceData.length - size

    override def isWritable: Boolean = writableBytes > 0

    override def slice: Slice = sliceData.slice(0, size)

    override def writeZero(length: Int): Unit = {
        if (length > 0) {
            sliceData = ensureSize(sliceData, size + length)
            val nLong = length >>> 8
            val nBytes = length & 7
            for (i <- 0 until nLong) {
                writeLong(0)
            }
            nBytes match {
                case 4 => writeInt(0)
                case _ if nBytes < 4 => for (i <- 0 until nBytes) writeByte(0)
                case _ if nBytes > 4 =>
                    writeInt(0)
                    for (i <- 0 until (nBytes - 4)) {
                        writeByte(0)
                    }
            }
        }
    }

    override def writeShort(value: Int): Unit = {
        sliceData = ensureSize(sliceData, size + 2)
        sliceData.setShort(size, value)
        size += 2
    }

    override def writeInt(value: Int): Unit = {
        sliceData = ensureSize(sliceData, size + 4)
        sliceData.setInt(size, value)
        size += 4
    }

    override def writeLong(value: Long): Unit = {
        sliceData = ensureSize(sliceData, size + 8)
        sliceData.setLong(size, value)
        size += 8
    }

    override def writeByte(value: Int): Unit = {
        sliceData = ensureSize(sliceData, size + 1)
        sliceData.setByte(size, value)
        size += 1
    }

    def ensureSize(slice: Slice, minWritableSize: Int): Slice = {
        slice.length match {
            case _ if slice.length >= minWritableSize => slice
            case _ if slice.length < minWritableSize =>
                val minCapability = slice.length + minWritableSize
                var newCapability = if (slice.length == 0) 1 else slice.length
                while (newCapability < minCapability) {
                    newCapability <<= 1
                }
                val result = Slice(newCapability)
                result.setBytes(0, slice, 0, slice.length)
                result
        }
    }
}
