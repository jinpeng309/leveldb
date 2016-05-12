package com.capslock.leveldb

import com.capslock.leveldb.SizeOf._

/**
 * Created by capslock.
 */
class InternalKey(userKey: Slice, sequenceNumber: Long, valueType: ValueType) {
    var hash = 0

}

object InternalKey {
    def apply(data: Slice): InternalKey = {
        val userKey = getUserKey(data)
        val packedSequenceNumberAndValueType = data.getLong(data.length - SIZE_OF_LONG)
        val sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceNumberAndValueType)
        val valueType = SequenceNumber.unpackValueType(packedSequenceNumberAndValueType)
        new InternalKey(userKey, sequenceNumber, valueType)
    }

    def getUserKey(data: Slice): Slice = {
        data.slice(0, data.length - SIZE_OF_LONG)
    }
}
