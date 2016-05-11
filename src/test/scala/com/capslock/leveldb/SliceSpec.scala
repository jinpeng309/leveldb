package com.capslock.leveldb

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, _}

/**
  * Created by capslock.
  */
class SliceSpec extends FlatSpec with Matchers with MockFactory {

    "Slice construct from a byte array " should " length and offset should be matched" in {
        val len = 3
        val offset = 1

        val slice1 = Slice(Array.fill(len)(0.toByte))
        slice1.offset shouldBe 0
        slice1.length shouldBe len


        val slice2 = Slice(len)
        slice2.data.length shouldEqual len
        slice2.length shouldEqual len
        slice2.offset shouldEqual 0

        val slice3 = Slice(Array.fill(len)(0), len, offset)
        slice3.data.length shouldEqual len
        slice3.length shouldEqual len
        slice3.offset shouldEqual offset
    }

    "Slice getShort " should " from little endian" in {
        val data = Array(0.toByte, 1.toByte)
        val slice = Slice(data)
        slice.getShort(0) shouldEqual Math.pow(2, 8)
    }

    "Slice getInt " should "from little endian" in {
        val data = Array(0.toByte, 0.toByte, 0.toByte, 1.toByte)
        val slice = Slice(data)
        slice.getInt(0) shouldEqual Math.pow(2, 24)
    }

}

