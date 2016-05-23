package com.capslock.leveldb

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class VariableLengthQuantitySpec extends FlatSpec with Matchers with MockFactory {

    "variableLengthSize for Int " should "equal to assign length" in {
        testVariableLengthInt(0x0, 1)
        testVariableLengthInt(0xf, 1)
        testVariableLengthInt(0xff, 2)
        testVariableLengthInt(0xfff, 2)
        testVariableLengthInt(0xffff, 3)
        testVariableLengthInt(0xfffff, 3)
        testVariableLengthInt(0xffffff, 4)
        testVariableLengthInt(0xfffffff, 4)
        testVariableLengthInt(0xffffffff, 5)
    }

    "variableLengthSize for Long " should "equal to assign length" in {
        testVariableLengthLong(0x0L, 1)
        testVariableLengthLong(0xfL, 1)
        testVariableLengthLong(0xffL, 2)
        testVariableLengthLong(0xfffL, 2)
        testVariableLengthLong(0xffffL, 3)
        testVariableLengthLong(0xfffffL, 3)
        testVariableLengthLong(0xffffffL, 4)
        testVariableLengthLong(0xfffffffL, 4)
        testVariableLengthLong(0xffffffffL, 5)
        testVariableLengthLong(0xfffffffffL, 6)
        testVariableLengthLong(0xffffffffffL, 6)
        testVariableLengthLong(0xfffffffffffL, 7)
        testVariableLengthLong(0xffffffffffffL, 7)
        testVariableLengthLong(0xfffffffffffffL, 8)
        testVariableLengthLong(0xffffffffffffffL, 8)
        testVariableLengthLong(0xfffffffffffffffL, 9)
        testVariableLengthLong(0xffffffffffffffffL, 10)
    }

    "writeVariableLengthInt and readVariableLengthInt" should "match each other" in {
        testVariableWriteAndReadInt(0x0)
        testVariableWriteAndReadInt(0xf)
        testVariableWriteAndReadInt(0xff)
        testVariableWriteAndReadInt(0xfff)
        testVariableWriteAndReadInt(0xffff)
        testVariableWriteAndReadInt(0xfffff)
        testVariableWriteAndReadInt(0xffffff)
        testVariableWriteAndReadInt(0xfffffff)
        testVariableWriteAndReadInt(0xffffffff)
    }

    "writeVariableLengthLong and readVariableLengthLong" should "match each other" in {
        testVariableWriteAndReadLong(0x0L)
        testVariableWriteAndReadLong(0xfL)
        testVariableWriteAndReadLong(0xffL)
        testVariableWriteAndReadLong(0xfffL)
        testVariableWriteAndReadLong(0xffffL)
        testVariableWriteAndReadLong(0xfffffL)
        testVariableWriteAndReadLong(0xffffffL)
        testVariableWriteAndReadLong(0xfffffffL)
        testVariableWriteAndReadLong(0xffffffffL)
        testVariableWriteAndReadLong(0xfffffffffL)
        testVariableWriteAndReadLong(0xffffffffffL)
        testVariableWriteAndReadLong(0xfffffffffffL)
        testVariableWriteAndReadLong(0xffffffffffffL)
        testVariableWriteAndReadLong(0xfffffffffffffL)
        testVariableWriteAndReadLong(0xffffffffffffffL)
        testVariableWriteAndReadLong(0xfffffffffffffffL)
        testVariableWriteAndReadLong(0xffffffffffffffffL)
    }

    def testVariableWriteAndReadInt(value: Int): Unit = {
        val output = DynamicSliceOutput(10)
        VariableLengthQuantity.writeVariableLengthInt(value, output)
        output.size shouldEqual VariableLengthQuantity.variableLengthSize(value)
        VariableLengthQuantity.readVariableLengthInt(SliceInput(output.slice())) shouldEqual value
    }

    def testVariableWriteAndReadLong(value: Long): Unit = {
        val output = DynamicSliceOutput(10)
        VariableLengthQuantity.writeVariableLengthLong(value, output)
        output.size shouldEqual VariableLengthQuantity.variableLengthSize(value)
        VariableLengthQuantity.readVariableLengthLong(SliceInput(output.slice())) shouldEqual value
    }

    def testVariableLengthInt(value: Int, length: Int): Unit = {
        VariableLengthQuantity.variableLengthSize(value) shouldEqual length
    }

    def testVariableLengthLong(value: Long, length: Int): Unit = {
        VariableLengthQuantity.variableLengthSize(value) shouldEqual length
    }
}
