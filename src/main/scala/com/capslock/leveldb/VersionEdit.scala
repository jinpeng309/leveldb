package com.capslock.leveldb


import com.capslock.leveldb.ListMultiMap.ListMultiMapImplicit

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
    var newFiles: Map[Int, List[FileMetaData]] = Map()
    var deleteFiles: Map[Int, List[Long]] = Map()

    def addFile(level: Int, fileMetaData: FileMetaData): Unit = {
        newFiles = newFiles.addBinding(level, fileMetaData)
    }

    def addFile(level: Int, fileNumber: Long, fileSize: Long, smallest: InternalKey, largest: InternalKey): Unit = {
        addFile(level, FileMetaData(fileNumber, fileSize, smallest, largest))
    }

    def deleteFile(level: Int, fileNumber: Long): Unit = {
        deleteFiles = deleteFiles.addBinding(level, fileNumber)
    }

    def setCompactPoint(level: Int, internalKey: InternalKey): Unit = {
        compactPointers += (level -> internalKey)
    }
}

object VersionEdit {
    def apply(): VersionEdit = {
        new VersionEdit
    }

    def apply(slice: Slice): VersionEdit = {
        val sliceInput = SliceInput(slice)
        val versionEdit = new VersionEdit
        while (sliceInput.isReadable) {
            val persistentId = VariableLengthQuantity.readVariableLengthInt(sliceInput)
            for (tag <- VersionEditTag.getVersionEditTagByPersistentId(persistentId)) {
                tag.readValue(sliceInput, versionEdit)
            }
        }
        versionEdit
    }

    implicit class VersionEditToSliceImplicit(versionEdit: VersionEdit) {
        def toSlice: Slice = {
            val sliceOutput = DynamicSliceOutput(4096)
            VersionEditTag.values.foreach(tag => tag.writeValue(sliceOutput, versionEdit))
            sliceOutput.slice()
        }
    }

}

