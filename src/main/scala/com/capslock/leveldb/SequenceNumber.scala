package com.capslock.leveldb

import com.capslock.leveldb.ValueType.ValueType

/**
 * Created by capslock.
 */
object SequenceNumber {
    val MAX_SEQUENCE_NUMBER = (0x1L << 56) - 1

    def packSequenceAndValueType(sequenceNumber: Long, valueType: ValueType): Long = {
        sequenceNumber << 8 | valueType.id
    }

    def unpackSequenceNumber(packedData: Long): Long = {
        packedData >>> 8
    }

    def unpackValueType(packedData: Long): ValueType = {
        ValueType(packedData.toInt)
    }
}
