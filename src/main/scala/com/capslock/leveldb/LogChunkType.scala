package com.capslock.leveldb

/**
 * Created by capslock.
 */
object LogChunkType extends Enumeration {
    type LogChunkType = Value
    val ZERO_TYPE, FULL, FIRST, MIDDLE, LAST, EOF, BAD_CHUNK, UNKNOWN = Value

    def getChunkChecksum(logChunkType: LogChunkType, slice: Slice): Int = {
        val crc32 = new PureJavaCrc32C
        crc32.update(logChunkType.id)
        crc32.update(slice.data, slice.offset, slice.length)
        crc32.getMaskedValue
    }
}
