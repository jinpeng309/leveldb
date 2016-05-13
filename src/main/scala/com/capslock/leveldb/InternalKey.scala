package com.capslock.leveldb

import com.capslock.leveldb.SizeOf._

/**
 * Created by capslock.
 */
case class InternalKey(userKey: Slice, sequenceNumber: Long, valueType: ValueType)

object InternalKey {
    def getUserKey(data: Slice): Slice = {
        data.slice(0, data.length - SIZE_OF_LONG)
    }

    implicit def SliceToInternalKey(data: Slice): InternalKey = {
        val userKey = getUserKey(data)
        val packedSequenceNumberAndValueType = data.getLong(data.length - SIZE_OF_LONG)
        val sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceNumberAndValueType)
        val valueType = SequenceNumber.unpackValueType(packedSequenceNumberAndValueType)
        new InternalKey(userKey, sequenceNumber, valueType)
    }

    implicit def InternalKeyToSlice(internalKey: InternalKey): Slice = {
        val slice = Slice(internalKey.userKey.length + SIZE_OF_LONG)
        val sliceOutput = BasicSliceOutput(slice)
        sliceOutput.writeBytes(internalKey.userKey)
        sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(internalKey.sequenceNumber, internalKey.valueType))
        slice
    }
}