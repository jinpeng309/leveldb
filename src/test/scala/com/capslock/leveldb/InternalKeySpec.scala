package com.capslock.leveldb

import com.capslock.leveldb.comparator.BytewiseComparator
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class InternalKeySpec extends FlatSpec with Matchers with MockFactory {
    it should "match each other" in {
        import InternalKey._
        val userKey = Slice("userKey")
        val sequenceNumber = 1000000L
        val valueType = ValueType.VALUE

        val key = InternalKey(userKey, sequenceNumber, valueType)
        val encodeSlice = key.toSlice
        val decodeKey = encodeSlice.toInternalKey

        BytewiseComparator().compare(userKey, key.userKey) shouldEqual 0
        decodeKey.sequenceNumber shouldEqual key.sequenceNumber
        decodeKey.valueType shouldEqual key.valueType
    }
}
