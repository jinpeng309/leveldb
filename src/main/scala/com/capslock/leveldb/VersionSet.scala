package com.capslock.leveldb

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.capslock.leveldb.FileName.LongToFileNameImplicit
import com.capslock.leveldb.comparator.InternalKeyComparator
import com.google.common.collect.MapMaker

import scala.collection.immutable.TreeMap

/**
 * Created by capslock.
 */
class VersionSet(val databaseDir: File, val tableCache: TableCache, val internalKeyComparator: InternalKeyComparator) {
    val nextFileNumber = new AtomicInteger(2)
    var manifestFileNumber: Long = 1
    var lastSequence: Long = 0
    var logNumber: Long = 0
    var preLogNumber: Long = 0
    val compactPointers = TreeMap[Int, InternalKey]()
    private val activeVersions = new MapMaker().weakKeys.makeMap()[Version, Object]
    var current: Version = new Version(this)
    activeVersions.put(current, new Object)
    var descriptorLog = Option.empty[LogWriter]


    def initializeIfNeeded(): Unit = {
        import VersionEdit.VersionEditToSliceImplicit
        val currentFile = new File(databaseDir, FileName.currentFileName)

        if (!currentFile.exists()) {
            val edit = VersionEdit()
            edit.comparatorName = Option(internalKeyComparator.name())
            edit.logNumber = Option(preLogNumber)
            edit.nextFileNumber = Option(nextFileNumber.get())
            edit.lastSequenceNumber = Option(lastSequence)

            val logWriter = MMapLogWriter(new File(databaseDir, manifestFileNumber.toDescriptorFileName),
                manifestFileNumber)
            try {
                writeSnapshot(logWriter)
                logWriter.addRecord(edit.toSlice, false)
            } finally {
                logWriter.close()
            }
        }
    }

    private def writeSnapshot(logWriter: LogWriter): Unit = {
        val edit = VersionEdit()
        edit.comparatorName = Option(internalKeyComparator.name())
        edit.compactPointers = compactPointers
        current.getFiles().foreach {
            case (level, fileMetaDataList) =>
                fileMetaDataList.foreach(fileMetaData => edit.addFile(level, fileMetaData))
        }
        logWriter.addRecord(edit.toSlice, false)
    }
}

object VersionSet {
    val L0_COMPACTION_TRIGGER: Int = 4
    val TARGET_FILE_SIZE: Int = 2 * 1048576
    val MAX_GRAND_PARENT_OVERLAP_BYTES: Long = 10 * TARGET_FILE_SIZE
}