package com.capslock.leveldb.comparator

import com.capslock.leveldb.{InternalKey, Slice, VALUE}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class InternalKeyComparatorSpec extends FlatSpec with Matchers with MockFactory {
    "compare two internalKey when userKey is equal result " should "dependent on sequenceNumber " in {
        val slice = Slice(3)
        val key1 = InternalKey(slice, 1, VALUE)
        val key2 = InternalKey(slice, 2, VALUE)
        val keyComparator = InternalKeyComparator(BytewiseComparator())
        assert(keyComparator.compare(key1, key2) > 0)
    }
}
