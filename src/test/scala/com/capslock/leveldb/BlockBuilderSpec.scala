package com.capslock.leveldb

import com.capslock.leveldb.comparator.BytewiseComparator
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class BlockBuilderSpec extends FlatSpec with Matchers with MockFactory {
    val estimate = 100
    val blockRestartInternal = 3
    val comparator = BytewiseComparator()
    val builder = BlockBuilder(estimate, blockRestartInternal, comparator)

    it should "init state" in {
        assert(builder.isEmpty)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT)
        assert(builder.entryCount == 0)
        assert(!builder.finished)
    }

    it should "increase entryCount and estimate after add record" in {
        val key = Slice("aaa")
        val value = Slice("aaa")
        builder.add(key, value)
        assert(!builder.isEmpty)
        assert(builder.entryCount == 1)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT + 3 + 3 + 3 + SizeOf.SIZE_OF_INT)

        builder.add(Slice("bbbb"), Slice("bbbb"))
        assert(!builder.isEmpty)
        assert(builder.entryCount == 2)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT + (3 + 3 + 3) + (3 + 4 + 4) + SizeOf.SIZE_OF_INT)
    }

    it should "share the key in restart range" in {
        builder.add(Slice("bbc"), Slice("bbc"))
        assert(!builder.isEmpty)
        assert(builder.entryCount == 3)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT + (3 + 3 + 3) + (3 + 4 + 4) + (3 + 1 + 3) + SizeOf.SIZE_OF_INT)
    }

    it should "not share the key when entryCount >= blockRestartInternal" in {
        builder.add(Slice("bbd"), Slice("bbd"))
        assert(!builder.isEmpty)
        assert(builder.entryCount == 4)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT * 2 + (3 + 3 + 3) + (3 + 4 + 4) + (3 + 1 + 3) + (3 + 3 + 3) + SizeOf.SIZE_OF_INT)
    }

    it should "throw exception if key <= lastKey" in {
        intercept[IllegalArgumentException] {
            builder.add(Slice("bbd"), Slice("bbd"))
        }
    }

    it should "be finished state after call finish method" in {
        builder.finish()
        assert(builder.finished)
    }

    it should "throw exception if add record to finished builder " in {
        intercept[IllegalArgumentException] {
            builder.add(Slice("ccccc"), Slice("ccccc"))
        }
    }

    it should "reset to init state after reset method " in {
        builder.reset()
        assert(builder.isEmpty)
        assert(builder.currentEstimate == SizeOf.SIZE_OF_INT)
        assert(builder.entryCount == 0)
        assert(!builder.finished)
    }
}
