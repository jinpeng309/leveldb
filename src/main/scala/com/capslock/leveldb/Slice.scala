package com.capslock.leveldb

import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by alvin.
  */
final class Slice(val data: Array[Byte], val length: Int, val offset: Int) extends Comparable[Slice] {
    var hash = 0

    def getByte(index: Int): Byte = {
        data(index + offset)
    }

    def getUnsignedByte(index: Int): Short = {
        getByte(index + offset).toShort
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

    override def compareTo(slice: Slice): Int = {
        if (this.equals(slice)) {
            0
        } else {
            val minLength = Math.min(length, slice.length)
            for (i <- 0.until(minLength)) {
                val v1 = getByte(i)
                val v2 = slice.getByte(i)
                if (v1 != v2) {
                    return v1 - v2
                }
            }
            length - slice.length
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
    def apply(length: Int): Slice = {
        new Slice(Array.fill(length)(0), length, 0)
    }

    def apply(data: Array[Byte]): Slice = {
        new Slice(data, data.length, 0)
    }

    def apply(data: Array[Byte], length: Int, offset: Int) = {
        new Slice(data, length, offset)
    }
}
