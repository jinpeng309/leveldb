package com.capslock.leveldb

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class BlockHandleSpec extends FlatSpec with Matchers with MockFactory {

    "readBlockHandle and writeBlockHandle " should "match each other" in {
        val offset = 10000000000L
        val dataSize = 10000000000L
        val blockHandle = BlockHandle(offset, dataSize)
        val slice = BlockHandle.writeBlockHandle(blockHandle)
        val result = BlockHandle.readBlockHandle(SliceInput(slice))

        result shouldEqual blockHandle
    }

}
