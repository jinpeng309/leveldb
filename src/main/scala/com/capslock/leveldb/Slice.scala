package com.capslock.leveldb

/**
  * Created by alvin.
  */
final class Slice(val data: Array[Byte], val length: Int, val offset: Int) extends Comparable[Slice] {
    var hash = 0

    def getByte(index: Int): Byte = {
        data(index + offset)
    }

    def getUnsignedByte(index: Int): Short = {
        getByte(index + offset).asInstanceOf[Short]
    }

    def getShort(index: Int): Short = {
        val innerIndex = index + offset
        (getByte(innerIndex) | (getByte(innerIndex + 1) << 8)).asInstanceOf[Short]
    }

    def getInt(index: Int): Int = {
        val innerIndex = index + offset
        getByte(innerIndex) |
            (getByte(innerIndex + 1) << 8) |
            (getByte(innerIndex + 2) << 16) |
            (getByte(innerIndex + 3) << 24)
    }

    def getByteAsLong(index: Int): Long = {
        getByte(index).asInstanceOf[Long]
    }

    def getLong(index: Int): Long = {
        val innerIndex = index + offset
        getByteAsLong(innerIndex) |
            (getByteAsLong(innerIndex + 1) << 8) |
            (getByteAsLong(innerIndex + 2) << 16) |
            (getByteAsLong(innerIndex + 3) << 24) |
            (getByteAsLong(innerIndex + 4) << 32) |
            (getByteAsLong(innerIndex + 5) << 40) |
            (getByteAsLong(innerIndex + 6) << 48) |
            (getByteAsLong(innerIndex + 7) << 56)
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
