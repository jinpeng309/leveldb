package com.capslock.leveldb

import org.scalatest.{FlatSpec, _}

/**
 * Created by capslock.
 */
class SliceSpec extends FlatSpec with Matchers {
    "Slice construct" should "" in {
        val slice = Slice(Array.fill(3)(0.toByte))
        slice.offset shouldBe 0
        slice.length shouldBe 3
    }
}
