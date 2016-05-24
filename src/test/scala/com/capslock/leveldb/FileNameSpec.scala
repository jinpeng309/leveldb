package com.capslock.leveldb

import java.io.File

import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by capslock.
 */
class FileNameSpec extends FlatSpec with Matchers with MockFactory {

    import FileName._

    val fileNumber = 100L
    it should " map long file number to special file name" in {
        fileNumber.toTableFileName shouldEqual "000100.sst"
        fileNumber.toLogFileName shouldEqual "000100.log"
        fileNumber.toDescriptorFileName shouldEqual "MANIFEST-000100"
        fileNumber.toDatabaseTempFileName shouldEqual "000100.dbtmp"
    }

    it should "extract fileInfo from file name" in {
        new File("000100.sst").toFileInfo shouldEqual FileInfo(FileType.TABLE, fileNumber)
        new File("000100.log").toFileInfo shouldEqual FileInfo(FileType.LOG, fileNumber)
        new File("MANIFEST-000100").toFileInfo shouldEqual FileInfo(FileType.DESCRIPTOR, fileNumber)
        new File("000100.dbtmp").toFileInfo shouldEqual FileInfo(FileType.TEMP, fileNumber)
        new File("CURRENT").toFileInfo shouldEqual FileInfo(FileType.CURRENT)
        new File("LOG").toFileInfo shouldEqual FileInfo(FileType.INFO_LOG)
        new File("LOG.old").toFileInfo shouldEqual FileInfo(FileType.INFO_LOG)
        new File("LOCK").toFileInfo shouldEqual FileInfo(FileType.DB_LOCK)
    }
}
