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
class VersionSet(val databaseDir: File, val tableCache: TableCache, val internalKeyComparator: InternalKeyComparator)
    extends SeekingIterable[InternalKey, Slice] {
    val userComparator = internalKeyComparator.userComparator
    val nextFileNumber = new AtomicInteger(2)
    var manifestFileNumber: Long = 1
    var lastSequence: Long = 0
    var logNumber: Long = 0
    var preLogNumber: Long = 0
    val compactPointers = TreeMap[Int, InternalKey]()
    private val activeVersions = new MapMaker().weakKeys.makeMap[Version, Object]()
    var current: Option[Version] = Option(new Version(this))
    activeVersions.put(current.get, new Object)
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
                logWriter.addRecord(edit.toSlice, force = false)
            } finally {
                logWriter.close()
            }
            FileName.setCurrentFile(databaseDir, manifestFileNumber)
        }
    }

    def destroy(): Unit = {
        for (log <- descriptorLog) {
            log.close()
            descriptorLog = Option.empty
        }

        for (currentVersion <- current) {
            currentVersion.release()
            current = Option.empty
        }
    }

    def appendVersion(version: Version): Unit = {
        val previous = current
        current = Option(version)
        activeVersions.put(version, new Object)
        for (previousVersion <- previous) {
            previousVersion.release()
        }
    }

    def removeVersion(version: Version): Unit = {
        activeVersions.remove(version)
    }


    def getNextFileNumber(): Int = nextFileNumber.incrementAndGet()


    private def writeSnapshot(logWriter: LogWriter): Unit = {
        val edit = VersionEdit()
        edit.comparatorName = Option(internalKeyComparator.name())
        edit.compactPointers = compactPointers
        for (currentVersion <- current) {
            currentVersion.getFiles().foreach((entry: (Int, List[FileMetaData])) => {
                val level = entry._1
                val fileList = entry._2
                fileList.foreach(file => edit.addFile(level, file))
            })
        }

        logWriter.addRecord(edit.toSlice, force = false)
    }

    def getOverlappingInputs(level: Int, smallest: Slice, largest: Slice): List[FileMetaData] = {
        for (currentVersion <- current) {
            return currentVersion.getFiles(level)
                .filter(file => userComparator.compare(file.largest.userKey, smallest) >= 0 &&
                    userComparator.compare(file.smallest.userKey, largest) <= 0)
        }
        List()
    }

    override def iterator(): SeekingIterator[InternalKey, Slice] = {
        require(current.isDefined, "current version is empty")
        current.get.iterator()
    }
}

object VersionSet {
    val L0_COMPACTION_TRIGGER: Int = 4
    val TARGET_FILE_SIZE: Int = 2 * 1048576
    val MAX_GRAND_PARENT_OVERLAP_BYTES: Long = 10 * TARGET_FILE_SIZE
}