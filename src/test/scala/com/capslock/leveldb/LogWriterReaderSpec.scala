package com.capslock.leveldb

import java.io.{File, FileInputStream}

import com.capslock.leveldb.comparator.BytewiseComparator
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class LogWriterReaderSpec extends FlatSpec with Matchers {

    import FileName._

    def testReadAndWrite(fileNumber: Long, sliceData: Slice): Unit = {
        val file = new File(fileNumber.toLogFileName)
        val logWriter = MMapLogWriter(file, fileNumber)
        logWriter.addRecord(sliceData, force = true)
        logWriter.close()

        val fileChannel = new FileInputStream(file).getChannel
        val logReader = LogReader(fileChannel, verifyChecksum = false, 0)
        val slice = logReader.readRecord()
        slice.isDefined shouldBe true
        BytewiseComparator().compare(sliceData, slice.get) shouldEqual 0
        fileChannel.close()
        file.delete()
    }

    "LogWriter and LogReader" should "match each other" in {
        testReadAndWrite(1L, Slice("a"))
        testReadAndWrite(2L, Slice("a" * LogConstants.BLOCK_SIZE))
        testReadAndWrite(3L, Slice("a" * LogConstants.BLOCK_SIZE * 3))
    }
}
