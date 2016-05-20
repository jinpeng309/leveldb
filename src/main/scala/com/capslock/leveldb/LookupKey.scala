package com.capslock.leveldb

/**
 * Created by capslock.
 */
class LookupKey(val userKey: Slice, sequenceNumber: Long) {
    val key = InternalKey(userKey, sequenceNumber, ValueType.VALUE)
}

object LookupKey {
    def apply(userKey: Slice, sequenceNumber: Long): LookupKey = {
        new LookupKey(userKey, sequenceNumber)
    }
}
