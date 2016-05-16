package com.capslock.leveldb

import java.io.{InputStream, OutputStream}
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Created by capslock.
 */
final class Slice(val data: Array[Byte], val offset: Int, val length: Int) extends Comparable[Slice] {
    var hash = 0

    def getByte(index: Int): Byte = {
        data(index + offset)
    }

    def getUnsignedByte(index: Int): Short = {
        (getByte(index + offset) & 0xff).toShort
    }

    def getShort(index: Int): Short = {
        val innerIndex = index + offset
        (data(innerIndex) & 0xff | (data(innerIndex + 1) << 8)).toShort
    }

    def getInt(index: Int): Int = {
        val innerIndex = index + offset
        data(innerIndex) & 0xff |
            ((data(innerIndex + 1) & 0xff) << 8) |
            ((data(innerIndex + 2) & 0xff) << 16) |
            ((data(innerIndex + 3) & 0xff) << 24)
    }

    def getLong(index: Int): Long = {
        val innerIndex = index + offset
        (data(innerIndex) & 0xff).toLong |
            ((data(innerIndex + 1) & 0xff).toLong << 8) |
            ((data(innerIndex + 2) & 0xff).toLong << 16) |
            ((data(innerIndex + 3) & 0xff).toLong << 24) |
            ((data(innerIndex + 4) & 0xff).toLong << 32) |
            ((data(innerIndex + 5) & 0xff).toLong << 40) |
            ((data(innerIndex + 6) & 0xff).toLong << 48) |
            ((data(innerIndex + 7) & 0xff).toLong << 56)
    }

    def getBytes(index: Int, destArray: Array[Byte], destIndex: Int, length: Int): Unit = {
        val innerIndex = index + offset
        System.arraycopy(data, innerIndex, destArray, destIndex, length)
    }

    def getBytes(index: Int, slice: Slice, destIndex: Int, length: Int): Unit = {
        getBytes(index, slice.data, destIndex, length)
    }

    def getBytes(index: Int, length: Int): Array[Byte] = {
        val innerIndex = index + offset
        val result = Array.fill(length)(0.toByte)
        Array.copy(data, innerIndex, result, 0, length)
        result
    }

    def getBytes(index: Int, destination: ByteBuffer) = {
        val innerIndex = index + offset
        destination.put(data, innerIndex, Math.min(destination.remaining(), length))
    }

    def getBytes(index: Int, out: OutputStream, length: Int) = {
        val innerIndex = index + offset
        out.write(data, innerIndex, length)
    }

    def setShort(index: Int, value: Int) = {
        val innerIndex = index + offset
        data(innerIndex) = value.toByte
        data(innerIndex + 1) = (value >> 8).toByte
    }

    def setInt(index: Int, value: Int) = {
        val innerIndex = index + offset
        data(innerIndex) = value.toByte
        data(innerIndex + 1) = ((value >> 8) & 0xff).toByte
        data(innerIndex + 2) = ((value >> 16) & 0xff).toByte
        data(innerIndex + 3) = ((value >> 24) & 0xff).toByte
    }

    def setLong(index: Int, value: Long) = {
        val innerIndex = index + offset
        data(innerIndex) = value.toByte
        data(innerIndex + 1) = ((value >> 8) & 0xff).toByte
        data(innerIndex + 2) = ((value >> 16) & 0xff).toByte
        data(innerIndex + 3) = ((value >> 24) & 0xff).toByte
        data(innerIndex + 4) = ((value >> 32) & 0xff).toByte
        data(innerIndex + 5) = ((value >> 40) & 0xff).toByte
        data(innerIndex + 6) = ((value >> 48) & 0xff).toByte
        data(innerIndex + 7) = ((value >> 56) & 0xff).toByte
    }

    def setByte(index: Int, value: Int) = {
        val innerIndex = index + offset
        data(innerIndex) = (value & 0xff).toByte
    }

    def setBytes(index: Int, source: Array[Byte], sourceIndex: Int, length: Int): Unit = {
        val innerIndex = index + offset
        System.arraycopy(source, sourceIndex, data, innerIndex, length)
    }

    def setBytes(index: Int, source: Slice, sourceIndex: Int, length: Int): Unit = {
        setBytes(index, source.data, source.offset + sourceIndex, length)
    }

    def setBytes(index: Int, source: ByteBuffer): Unit = {
        val innerIndex = index + offset
        source.get(data, innerIndex, source.remaining)
    }

    def setBytes(index: Int, in: InputStream, length: Int): Int = {
        val innerIndex = index + offset
        var readLength = 0
        Stream.continually(in.read(data, innerIndex, length)).takeWhile(length => {
            readLength += length
            length != -1
        })
        if (readLength == 0) -1 else readLength
    }

    def setBytes(index: Int, in: FileChannel, position: Int, length: Int): Int = {
        val innerIndex = index + offset
        val byteBuffer = ByteBuffer.wrap(data, innerIndex, length)
        var readLength = 0
        Stream.continually(in.read(byteBuffer, position)).takeWhile(length => {
            readLength += length
            length != -1
        })
        if (readLength == 0) -1 else readLength
    }

    def copySlice(index: Int, length: Int): Slice = {
        val innerIndex = index + offset
        val copyData = Array.fill[Byte](length)(0)
        data.copyToArray(copyData, innerIndex)
        Slice(copyData)
    }

    def copyBytes(index: Int, length: Int): Array[Byte] = {
        val innerIndex = index + offset
        val copyData = Array.fill[Byte](length)(0)
        data.copyToArray(copyData, innerIndex)
        copyData
    }

    def slice(index: Int, length: Int): Slice = {
        Slice(data, offset + index, length)
    }

    def slice(): Slice = {
        slice(0, length)
    }


    override def compareTo(anotherSlice: Slice): Int = {
        if (this.equals(anotherSlice)) {
            0
        } else {
            val minLength = Math.min(length, anotherSlice.length)
            for (i <- 0.until(minLength)) {
                val v1 = getByte(i)
                val v2 = anotherSlice.getByte(i)
                if (v1 != v2) {
                    return v1 - v2
                }
            }
            length - anotherSlice.length
        }
    }


    override def hashCode(): Int = {
        if (hash == 0) {
            var result = length
            for (i <- offset.until(offset + length)) {
                result = result * 31 + data(i)
            }
            if (result == 0) {
                result = 1
            }
            hash = result
        }
        hash
    }


    override def equals(slice: scala.Any): Boolean = {
        slice match {
            case s: Slice => s.length == length && s.offset == offset && s.data.sameElements(data)
            case _ => false
        }
    }
}

object Slice {
    def empty = Slice(0)

    def apply(length: Int): Slice = {
        new Slice(Array.fill(length)(0), 0, length)
    }

    def apply(data: Array[Byte]): Slice = {
        new Slice(data, 0, data.length)
    }

    def apply(data: Array[Byte], offset: Int, length: Int) = {
        new Slice(data, offset, length)
    }

    def copiedBuffer(source: ByteBuffer, offset: Int, length: Int): Slice = {
        val newPosition = source.position() + offset
        copiedBuffer(source.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length)
            .position(newPosition).asInstanceOf[ByteBuffer])
    }


    def copiedBuffer(source: ByteBuffer): Slice = {
        val copy = Slice(source.limit() - source.position())
        copy.setBytes(0, source.duplicate().order(ByteOrder.LITTLE_ENDIAN))
        copy
    }

    def calculateCommonBytes(sliceA: Option[Slice], sliceB: Option[Slice]): Int = {
        var commonBytes = 0
        for (leftKey <- sliceA; rightKey <- sliceB) {
            val minLength = Math.min(leftKey.length, rightKey.length)
            while (commonBytes > minLength && leftKey.getUnsignedByte(commonBytes) == rightKey.getUnsignedByte(commonBytes)) {
                commonBytes += 1
            }
        }
        commonBytes
    }
}
