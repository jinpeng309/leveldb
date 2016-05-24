package com.capslock.leveldb

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class SequenceNumberSpec extends FlatSpec with Matchers {
    val sequenceNumber = 100000000000L
    val valueType = ValueType.VALUE
    it should "match each other" in {
        val packedValue = SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType)
        SequenceNumber.unpackSequenceNumber(packedValue) shouldEqual sequenceNumber
        SequenceNumber.unpackValueType(packedValue) shouldEqual valueType
    }
}
