package com.capslock.leveldb


import scala.collection.immutable.TreeMap
import scala.collection.mutable

/**
 * Created by capslock.
 */
class VersionEdit {
    var comparatorName: Option[String] = None
    var logNumber: Option[Long] = None
    var nextFileNumber: Option[Long] = None
    var previousLogNumber: Option[Long] = None
    var lastSequenceNumber: Option[Long] = None
    var compactPointers = TreeMap[Int, InternalKey]()
    val newFiles = new mutable.HashMap[Int, List[FileMetaData]] with mutable.MultiMap[Int, FileMetaData]
    val deleteFiles = new mutable.HashMap[Int, List[Int]] with mutable.MultiMap[Int, Long]

    def addFile(level: Int, fileMetaData: FileMetaData): Unit = {
        newFiles.addBinding(level, fileMetaData)
    }

    def addFile(level: Int, fileNumber: Long, fileSize: Long, smallest: InternalKey, largest: InternalKey): Unit = {
        addFile(level, FileMetaData(fileNumber, fileSize, smallest, largest))
    }

    def deleteFile(level: Int, fileNumber: Long): Unit = {
        deleteFiles.addBinding(level, fileNumber)
    }

    def setCompactPoint(level: Int, internalKey: InternalKey): Unit = {
        compactPointers += (level -> internalKey)
    }
}
