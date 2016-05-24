package com.capslock.leveldb

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class BlockTrailerSpec extends FlatSpec with Matchers with MockFactory {
    "readBlockTrailer and writeBlockTrailer" should "match each other" in {
        val crc32 = 100000
        val snappyCompressType = CompressionType.SNAPPY
        val blockTrailer = BlockTrailer(crc32, snappyCompressType)
        val slice = BlockTrailer.writeBlockTrailer(blockTrailer)
        val result = BlockTrailer.readBlockTrailer(slice)

        result shouldEqual blockTrailer
    }
}
