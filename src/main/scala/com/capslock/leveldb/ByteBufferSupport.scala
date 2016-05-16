package com.capslock.leveldb

import java.nio.MappedByteBuffer

import sun.nio.ch.FileChannelImpl

/**
 * Created by capslock.
 */
object ByteBufferSupport {
    val unmapMethod = classOf[FileChannelImpl].getDeclaredMethod("unmap", classOf[MappedByteBuffer])
    unmapMethod.setAccessible(true)

    def unmap(buffer: MappedByteBuffer): Unit = {
        unmapMethod.invoke(null, buffer)
    }
}
