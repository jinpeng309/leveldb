package com.capslock.leveldb

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.capslock.leveldb.FileName.LongToFileNameImplicit
import com.capslock.leveldb.comparator.InternalKeyComparator
import com.google.common.collect.{ComparisonChain, MapMaker}

import scala.collection.immutable.{HashSet, TreeMap, TreeSet}

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
    var compactPointers = TreeMap[Int, InternalKey]()
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

    def get(lookupKey: LookupKey): Option[LookupResult] = {
        current.flatMap(version => version.get(lookupKey)).orElse(Option.empty)
    }

    def numberOfFilesInLevel(level: Int): Int = {
        current.map(version => version.numberOfFilesInLevel(level)).getOrElse(-1)
    }
}

case class LevelState(internalKeyComparator: InternalKeyComparator) {
    val fileMetaDataOrdering = VersionSet.getFileMetaDataOrdering(internalKeyComparator)
    var addedFiles = new TreeSet[FileMetaData]()(fileMetaDataOrdering)
    var deletedFiles = HashSet[Long]()

}

class Builder(versionSet: VersionSet, baseVersion: Version) {
    val levels: List[LevelState] = List.fill(DbConstants.NUM_LEVELS)(LevelState(versionSet.internalKeyComparator))

    def apply(versionEdit: VersionEdit): Unit = {
        versionEdit.compactPointers.foreach(entry => versionSet.compactPointers += entry)

        versionEdit.deleteFiles.foreach {
            case (level: Int, fileList: List[Long]) =>
                levels(level).deletedFiles ++= fileList
        }
        // We arrange to automatically compact this file after
        // a certain number of seeks.  Let's assume:
        //   (1) One seek costs 10ms
        //   (2) Writing or reading 1MB costs 10ms (100MB/s)
        //   (3) A compaction of 1MB does 25MB of IO:
        //         1MB read from this level
        //         10-12MB read from next level (boundaries may be misaligned)
        //         10-12MB written to next level
        // This implies that 25 seeks cost the same as the compaction
        // of 1MB of data.  I.e., one seek costs approximately the
        // same as the compaction of 40KB of data.  We are a little
        // conservative and allow approximately one seek for every 16KB
        // of data before triggering a compaction.
        versionEdit.newFiles.foreach {
            case (level: Int, fileList: List[FileMetaData]) =>
                fileList.foreach(file => {
                    var allowedSeeks = file.fileSize / 16384
                    if (allowedSeeks < 100) {
                        allowedSeeks = 100
                    }
                    file.allowedSeeks = allowedSeeks.toInt
                    levels(level).deletedFiles -= file.fileNumber
                    levels(level).addedFiles += file
                })
        }
    }

    def saveTo(version: Version): Unit = {
        val comparator = VersionSet.getFileMetaDataOrdering(versionSet.internalKeyComparator)
        baseVersion.getFiles().foreach {
            case (level: Int, files: List[FileMetaData]) =>
                val addedFiles = levels(level).addedFiles
                val sortedFiles = files ::: addedFiles.toList
                sortedFiles.sorted(comparator)
                    .foreach(file => maybeAddFile(version, level, file))


        }
    }

    def maybeAddFile(version: Version, level: Int, fileMetaData: FileMetaData): Unit = {
        if (!levels(level).deletedFiles.contains(fileMetaData.fileNumber)) {
            val files = version.getFiles(level)
            if (level > 0 && files.nonEmpty) {
                val fileOverlap = versionSet.internalKeyComparator.compare(files.last.largest, fileMetaData.smallest) >= 0
                require(!fileOverlap,
                    s"Compaction is obsolete files :${files.last.largest}, ${fileMetaData.fileNumber}, level = $level")
            }
            version.addFile(level, fileMetaData)
        }
    }

}

object VersionSet {
    val L0_COMPACTION_TRIGGER: Int = 4
    val TARGET_FILE_SIZE: Int = 2 * 1048576
    val MAX_GRAND_PARENT_OVERLAP_BYTES: Long = 10 * TARGET_FILE_SIZE

    def getFileMetaDataOrdering(internalKeyComparator: InternalKeyComparator): Ordering[FileMetaData] = {
        new Ordering[FileMetaData] {
            override def compare(f1: FileMetaData, f2: FileMetaData): Int = {
                ComparisonChain
                    .start
                    .compare(f1.smallest, f2.smallest, internalKeyComparator)
                    .compare(f1.fileNumber, f2.fileNumber)
                    .result
            }
        }
    }
}