package com.capslock.leveldb

import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class FooterSpec extends FlatSpec with Matchers with MockFactory {
    "readFooter writeFooter" should "match each other" in {
        val metaIndexBlockHandle = BlockHandle(100, 100)
        val indexBlockHandle = BlockHandle(200, 100)
        val output = DynamicSliceOutput(10)
        val inputFooter = Footer(metaIndexBlockHandle, indexBlockHandle)
        Footer.writeFooter(inputFooter, output)
        val slice = output.slice()
        val resultFooter = Footer.readFooter(slice)
        resultFooter shouldEqual inputFooter
    }
}
