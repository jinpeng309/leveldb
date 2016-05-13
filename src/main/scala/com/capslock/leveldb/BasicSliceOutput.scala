package com.capslock.leveldb

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
  * Created by capslock.
  */
class BasicSliceOutput(sliceData: Slice) extends SliceOutput {
    var size = 0

    override def writeShort(value: Int): Unit = {
        sliceData.setShort(size, value)
        size += 2
    }

    override def writeInt(value: Int): Unit = {
        sliceData.setInt(size, value)
        size += 4
    }

    override def writeLong(value: Long): Unit = {
        sliceData.setLong(size, value)
        size += 8
    }

    override def writeByte(value: Int): Unit = {
        sliceData.setByte(size, value)
        size += 1
    }

    override def reset(): Unit = size = 0

    override def writeBytes(source: Slice): Unit = writeBytes(source, 0, slice.length)

    override def writeBytes(source: Slice, length: Int): Unit = writeBytes(source, 0, length)

    override def writeBytes(source: Slice, index: Int, length: Int): Unit = {
        sliceData.setBytes(size, source, index, length)
        size += length
    }

    override def writeBytes(source: Array[Byte]): Unit = writeBytes(source, 0, source.length)

    override def writeBytes(source: Array[Byte], sourceIndex: Int, length: Int): Unit = {
        sliceData.setBytes(size, source, sourceIndex, length)
        size += length
    }

    override def writeBytes(source: ByteBuffer): Unit = {
        val length = source.remaining
        sliceData.setBytes(size, source)
        size += length
    }

    override def writeBytes(source: InputStream, length: Int): Unit = {
        val writeSize = sliceData.setBytes(size, source, length)
        size += writeSize
    }

    override def writeBytes(source: SliceInput, length: Int) : Unit = {
        if (length > source.available) {
            throw new IndexOutOfBoundsException
        }
        writeBytes(source.readBytes(length))
    }

    override def writeZero(length: Int): Unit = {
        if (length > 0) {
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

    override def writeBytes(source: FileChannel, position: Int, length: Int): Unit = {
        val len = sliceData.setBytes(size, source, position, length)
        size += len
    }

    override def writableBytes: Int = sliceData.length - size

    override def isWritable: Boolean = writableBytes > 0

    override def slice(): Slice = sliceData.slice(0, size)
}

object BasicSliceOutput {
    def apply(slice: Slice): BasicSliceOutput = {
        new BasicSliceOutput(slice)
    }
}
