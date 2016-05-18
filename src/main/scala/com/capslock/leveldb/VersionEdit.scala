package com.capslock.leveldb

import com.google.common.collect.{ArrayListMultimap, Multimap}

import scala.collection.immutable.TreeMap

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
    val newFiles = ArrayListMultimap.create[Integer, FileMetaData]()
    val deleteFiles = ArrayListMultimap.create[Integer, Long]()

    def addFile(level: Int, fileMetaData: FileMetaData): Unit = {
        newFiles.put(level, fileMetaData)
    }

    def addFile(level: Int, fileNumber: Long, fileSize: Long, smallest: InternalKey, largest: InternalKey): Unit = {
        addFile(level, FileMetaData(fileNumber, fileSize, smallest, largest))
    }

    def addFiles(files: Multimap[Integer, FileMetaData]): Unit = {
        newFiles.putAll(files)
    }

    def deleteFile(level: Int, fileNumber: Long): Unit = {
        deleteFiles.put(level, fileNumber)
    }

    def setCompactPoint(level: Int, internalKey: InternalKey): Unit = {
        compactPointers += (level -> internalKey)
    }
}
