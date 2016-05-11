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
        assert(slice.getShort(0) == Math.pow(2, 8).toShort)
    }

    "Slice getInt " should "from little endian" in {
        val data = Array(0.toByte, 0.toByte, 0.toByte, 1.toByte)
        val slice = Slice(data)
        assert(slice.getInt(0) == Math.pow(2, 24).toLong)
    }

    "Slice getLong" should "from little endian" in {
        val data = Array(0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 0.toByte, 1.toByte)
        val slice = Slice(data)
        assert(slice.getLong(0) == Math.pow(2, 56).toLong)
    }

    "Same slice " should " equal to each other" in {
        val data1 = Array(0.toByte, 1.toByte)
        val slice1 = Slice(data1)
        val data2 = Array(0.toByte, 1.toByte)
        val slice2 = Slice(data2)

        slice1.equals(slice2) shouldBe true
        slice2.equals(slice1) shouldBe true
    }

    "Same slice hashCode " should " equal to each other " in {
        val data1 = Array(0.toByte, 1.toByte)
        val slice1 = Slice(data1)
        val data2 = Array(0.toByte, 1.toByte)
        val slice2 = Slice(data2)

        slice1.hashCode() shouldEqual slice2.hashCode()
        slice2.hashCode() shouldEqual slice1.hashCode()
    }

    it should " true " in {
        val data1 = Array(0.toByte, 1.toByte)
        val slice1 = Slice(data1)
        val data2 = Array(1.toByte, 1.toByte)
        val slice2 = Slice(data2)

        assert(slice1.compareTo(slice2) <= -1)
        assert(slice2.compareTo(slice1) > 0)
    }

    "Compare same slice " should " equal to each other" in {
        val data1 = Array(0.toByte, 1.toByte)
        val slice1 = Slice(data1)
        val data2 = Array(0.toByte, 1.toByte)
        val slice2 = Slice(data2)

        assert(slice1.compareTo(slice2) == 0)
        assert(slice2.compareTo(slice1) == 0)
    }
}

