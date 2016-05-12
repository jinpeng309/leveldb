package com.capslock.leveldb

import java.io.InputStream

/**
 * Created by capslock.
 */
final class SliceInput(slice: Slice) extends InputStream {
    var position = 0

    def isReadable = available > 0

    def readByte = {
        position += 1
        slice.getByte(position - 1)
    }

    def readBoolean = {
        readByte != 0
    }

    def readUnsignedByte: Int = {
        readByte & 0xff
    }

    def readShort = {
        position += 2
        slice.getShort(position - 2)
    }

    def readInt = {
        position += 4
        slice.getInt(position - 4)
    }

    def readLong = {
        position += 8
        slice.getLong(position - 8)
    }


    override def available = slice.length - position

    override def read(): Int = {
        readByte
    }
}
