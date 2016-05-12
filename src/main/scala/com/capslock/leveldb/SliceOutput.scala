package com.capslock.leveldb

import java.io.{DataOutput, OutputStream}

/**
 * Created by capslock.
 */
abstract class SliceOutput extends OutputStream with DataOutput {
    override def writeFloat(v: Float): Unit = throw new UnsupportedOperationException

    override def writeChars(s: String): Unit = throw new UnsupportedOperationException

    override def writeDouble(v: Double): Unit = throw new UnsupportedOperationException

    override def writeUTF(s: String): Unit = throw new UnsupportedOperationException

    override def writeBoolean(value: Boolean): Unit = writeByte(if (value) 1 else 0)

    override def write(value: Int): Unit = writeByte(value)

    override def writeBytes(s: String): Unit = throw new UnsupportedOperationException

    override def writeChar(v: Int): Unit = throw new UnsupportedOperationException
}
