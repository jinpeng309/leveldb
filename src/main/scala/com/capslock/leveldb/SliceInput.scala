package com.capslock.leveldb

import java.io.{DataInput, InputStream}

/**
 * Created by capslock.
 */
final class SliceInput(slice: Slice) extends InputStream with DataInput {
    var position = 0

    def isReadable = available > 0

    def readByte = {
        val result = slice.getByte(position)
        position += 1
        result
    }

    def readBoolean = {
        readByte != 0
    }

    override def readUnsignedShort: Int = {
        readShort & 0xff
    }

    override def readShort = {
        val result = slice.getShort(position)
        position += 2
        result
    }

    override def readInt = {

        val result = slice.getInt(position)
        position += 4
        result
    }

    override def readLong = {
        val result = slice.getLong(position)
        position += 8
        result
    }

    def readBytes(length: Int): Slice = {
        val result = slice.slice(position, length)
        position += length
        result
    }

    def readBytes(destination: Array[Byte], destIndex: Int, length: Int): Unit = {
        slice.getBytes(position, destination, destIndex, length)
        position += length
    }

    def readBytes(destination: Array[Byte]): Unit = {
        readBytes(destination, 0, destination.length)
    }

    override def readFully(data: Array[Byte]): Unit = {
        readBytes(data)
    }

    override def readFully(data: Array[Byte], offset: Int, length: Int): Unit = {
        readBytes(data, offset, length)
    }

    override def skipBytes(n: Int): Int = {
        val skipSize = Math.min(n, available)
        position += skipSize
        skipSize
    }


    override def available = slice.length - position

    override def read(): Int = {
        readByte
    }

    override def readChar(): Char = throw new UnsupportedOperationException

    override def readFloat(): Float = throw new UnsupportedOperationException

    override def readUTF(): String = throw new UnsupportedOperationException

    override def readLine(): String = throw new UnsupportedOperationException

    override def readDouble(): Double = throw new UnsupportedOperationException

    override def readUnsignedByte(): Int = readByte & 0xff
}
