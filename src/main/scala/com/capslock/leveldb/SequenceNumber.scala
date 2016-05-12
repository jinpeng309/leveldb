package com.capslock.leveldb

/**
 * Created by capslock.
 */
object SequenceNumber {
    def packSequenceAndValueType(sequenceNumber: Long, valueType: ValueType): Long = {
        sequenceNumber << 8 | ValueType.toByte(valueType)
    }

    def unpackSequenceNumber(packedData: Long): Long = {
        packedData >>> 8
    }

    def unpackValueType(packedData: Long): ValueType = {
        ValueType.fromByte(packedData.toByte)
    }
}
