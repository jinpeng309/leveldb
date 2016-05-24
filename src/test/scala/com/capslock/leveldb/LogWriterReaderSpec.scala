package com.capslock.leveldb

import java.io.{File, FileInputStream}

import com.capslock.leveldb.comparator.BytewiseComparator
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class LogWriterReaderSpec extends FlatSpec with Matchers {

    import FileName._

    "LogWriter and LogReader" should "match each other" in {

        val fileNumber = 100L
        val file = new File(fileNumber.toLogFileName)
        val sliceData = Slice("data")
        val logWriter = MMapLogWriter(file, fileNumber)
        logWriter.addRecord(sliceData, force = true)
        logWriter.close()

        val logReader = LogReader(new FileInputStream(file).getChannel, verifyChecksum = false, 0)
        val slice = logReader.readRecord()
        slice.isDefined shouldBe true
        BytewiseComparator().compare(sliceData, slice.get) shouldEqual 0
    }
}
