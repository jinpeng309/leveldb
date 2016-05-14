package com.capslock.leveldb

import com.capslock.leveldb.ValueType.ValueType

/**
 * Created by capslock.
 */
object SequenceNumber {
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
