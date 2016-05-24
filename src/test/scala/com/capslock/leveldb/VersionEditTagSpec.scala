package com.capslock.leveldb

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class VersionEditTagSpec extends FlatSpec with Matchers with MockFactory {
    "readValue writeValue method" should "match each other" in {
        VersionEditTag.values.foreach(versionEditTag => {
            testReadWrite(versionEditTag)
        })
    }

    def testReadWrite(versionEditTag: VersionEditTag): Unit = {
    }

}
