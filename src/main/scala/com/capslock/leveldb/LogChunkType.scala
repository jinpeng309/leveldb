package com.capslock.leveldb

/**
 * Created by capslock.
 */
class LogChunkType extends Enumeration {
    type LogChunkType = Value
    val ZERO_TYPE, FULL, FIRST, MIDDLE, LAST, EOF, BAD_CHUNK, UNKNOWN = Value
}
