package com.capslock.leveldb

import org.scalatest.{FlatSpec, _}

/**
 * Created by capslock.
 */
class SliceSpec extends FlatSpec with Matchers {
    "Slice construct from a byte array " should " length and offset should be matched" in {
        val slice = Slice(Array.fill(3)(0.toByte))
        slice.offset shouldBe 0
        slice.length shouldBe 3
    }
}
