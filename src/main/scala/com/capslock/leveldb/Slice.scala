package com.capslock.leveldb

/**
 * Created by alvin.
 */
final class Slice(val data: Array[Byte], val length: Int, val offset: Int) {
    def getByte(index: Int): Byte = {
        data(index + offset)
    }

    def getUnsignedByte(index: Int): Short = {
        getByte(index + offset).asInstanceOf[Short]
    }

    def getShort(index: Int): Short = {
        (getByte(index) | (getByte(index + 1) << 8)).asInstanceOf[Short]
    }

    def getInt(index: Int): Int = {
        getByte(index) |
            (getByte(index + 1) << 8) |
            (getByte(index + 2) << 16) |
            (getByte(index + 3) << 24)
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
