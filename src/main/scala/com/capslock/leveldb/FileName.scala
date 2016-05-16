package com.capslock.leveldb

import java.io.File

import com.capslock.leveldb.FileType.FileType

/**
 * Created by capslock.
 */
object FileName {
    val DescriptorFilePattern = "MANIFEST-(\\d+)".r
    val LogFilePattern = "(\\d+).log".r
    val TableFilePattern = "(\\d+).sst".r

    implicit class LongToFileNameImplicit(number: Long) {
        def toTableFileName: String = {
            "%06d.sst".format(number)
        }

        def toLogFileName: String = {
            "%06d.log".format(number)
        }

        def toDescriptorFileName: String = {
            "MANIFEST-%06d".format(number)
        }
    }

    implicit class FileToFileInfoImplicit(file: File) {
        def toFileInfo: FileInfo = {
            val fileName = file.getName
            fileName match {
                case "CURRENT" => FileInfo(FileType.CURRENT)
                case "LOCK" => FileInfo(FileType.DB_LOCK)
                case "LOG" => FileInfo(FileType.INFO_LOG)
                case "LOG.old" => FileInfo(FileType.INFO_LOG)
                case DescriptorFilePattern(fileNumber) => FileInfo(FileType.DESCRIPTOR, fileNumber.toLong)
                case LogFilePattern(fileNumber) => FileInfo(FileType.LOG, fileNumber.toLong)
                case TableFilePattern(fileNumber) => FileInfo(FileType.TABLE, fileNumber.toLong)
            }
        }
    }

    val currentFileName = "CURRENT"
    val lockFile = "LOCK"
}

object FileType extends Enumeration {
    type FileType = Value
    val LOG, DB_LOCK, TABLE, DESCRIPTOR, CURRENT, TEMP, INFO_LOG = Value
}

case class FileInfo(fileType: FileType, fileNumber: Long = 0)