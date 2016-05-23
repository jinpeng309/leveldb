package com.capslock.leveldb

import java.nio.ByteBuffer

/**
 * Created by capslock.
 */
object VariableLengthQuantity {
    def variableLengthSize(value: Int): Int = {
        var result = value
        var size = 1
        while ((result & (~0x7f)) != 0) {
            size += 1
            result = result >>> 7
        }
        size
    }

    def variableLengthSize(value: Long): Int = {
        var result = value
        var size = 1
        while ((result & (~0x7f)) != 0) {
            size += 1
            result = result >>> 7
        }
        size
    }

    def writeVariableLengthInt(value: Int, sliceOutput: SliceOutput): Unit = {
        var data = value
        while ((data & (~0x7f)) != 0) {
            sliceOutput.writeByte((data & 0x7f) | 0x80)
            data >>>= 7
        }
        sliceOutput.writeByte(data)
    }

    def writeVariableLengthLong(value: Long, sliceOutput: SliceOutput) {
        var data = value
        while ((data & (~0x7f)) != 0) {
            sliceOutput.writeByte(((data & 0x7f) | 0x80).toInt)
            data >>>= 7
        }
        sliceOutput.writeByte(data.toInt)
    }

    def readVariableLengthInt(sliceInput: SliceInput): Int = {
        var result = 0
        for (shift <- 0 to 28 by 7) {
            val b = sliceInput.readUnsignedByte()
            result |= ((b & 0x7f) << shift)
            if ((b & 0x80) == 0) {
                return result
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set")
    }

    def readVariableLengthInt(sliceInput: ByteBuffer): Int = {
        var result = 0
        for (shift <- 0 to 28 by 7) {
            val b = sliceInput.get()
            result |= ((b & 0x7f) << shift)
            if ((b & 0x80) == 0) {
                return result
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set")
    }


    def readVariableLengthLong(sliceInput: SliceInput): Long = {
        var result: Long = 0L
        for (shift <- 0 to 63 by 7) {
            val b = sliceInput.readUnsignedByte()
            result |= (b & 0x7f).toLong << shift
            if ((b & 0x80) == 0) {
                return result
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set")
    }

}
