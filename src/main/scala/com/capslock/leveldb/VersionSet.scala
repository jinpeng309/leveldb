package com.capslock.leveldb

import java.io.{File, FileInputStream}
import java.util.concurrent.atomic.AtomicLong

import com.capslock.leveldb.FileName.LongToFileNameImplicit
import com.capslock.leveldb.comparator.InternalKeyComparator
import com.google.common.base.Charsets
import com.google.common.collect.{ComparisonChain, MapMaker}
import com.google.common.io.Files

import scala.collection.immutable.{HashSet, TreeMap, TreeSet}

/**
 * Created by capslock.
 */
class VersionSet(val databaseDir: File, val tableCache: TableCache, val internalKeyComparator: InternalKeyComparator)
    extends SeekingIterable[InternalKey, Slice] {
    val userComparator = internalKeyComparator.userComparator
    val nextFileNumber = new AtomicLong(2)
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


    def getNextFileNumber(): Long = nextFileNumber.incrementAndGet()


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

    def logAndApply(edit: VersionEdit): Unit = {
        if (edit.logNumber.isEmpty) {
            edit.logNumber = Some(logNumber)
        }

        if (edit.previousLogNumber.isEmpty) {
            edit.previousLogNumber = Some(preLogNumber)
        }

        edit.nextFileNumber = Some(nextFileNumber.get())
        edit.lastSequenceNumber = Some(lastSequence)

        val version = Version(this)
        val builder = Builder(this, version)
        builder.apply(edit)
        builder.saveTo(version)

        finalizeVersion(version)

        var createNewManifest = false

        if (descriptorLog.isEmpty) {
            edit.nextFileNumber = Some(nextFileNumber.get())
            descriptorLog = Some(MMapLogWriter(new File(databaseDir, manifestFileNumber.toDescriptorFileName), manifestFileNumber))
            writeSnapshot(descriptorLog.get)
            createNewManifest = true
        }
        if (createNewManifest) {
            FileName.setCurrentFile(databaseDir, manifestFileNumber)
        }

        appendVersion(version)
        logNumber = edit.logNumber.getOrElse(-1)
        preLogNumber = edit.previousLogNumber.getOrElse(-1)
    }

    def recover(): Unit = {
        val currentFile = new File(databaseDir, FileName.currentFileName)
        var currentName = Files.toString(currentFile, Charsets.UTF_8)
        require(currentName.charAt(currentName.length - 1) == '\n', "CURRENT file does not end with newline")
        currentName = currentName.substring(0, currentName.length - 1)

        val fileChannel = new FileInputStream(new File(databaseDir, currentName)).getChannel
        try {
            var nextFileNumber = Option.empty[Long]
            var lastSequence = Option.empty[Long]
            var logNumber = Option.empty[Long]
            var preLogNumber = Option.empty[Long]
            val builder = Builder(this, current.get)

            val reader = new LogReader(fileChannel, true, 0)
            var record = reader.readRecord()

            while (record.isDefined) {
                val edit = VersionEdit(record.get)

                val editComparator = edit.comparatorName
                val stringComparator = internalKeyComparator.userComparator
                require(editComparator.isEmpty || editComparator.get == stringComparator,
                    s"Expected user comparator ${editComparator.get} to match existing database comparator")
                record = reader.readRecord()

                builder.apply(edit)

                nextFileNumber = edit.nextFileNumber.orElse(nextFileNumber)
                lastSequence = edit.lastSequenceNumber.orElse(lastSequence)
                logNumber = edit.logNumber.orElse(logNumber)
                preLogNumber = edit.previousLogNumber.orElse(preLogNumber)
            }

            val version = Version(this)
            builder.saveTo(version)
            finalizeVersion(version)

            appendVersion(version)
            manifestFileNumber = nextFileNumber.getOrElse(0)
            this.nextFileNumber.set(nextFileNumber.getOrElse(0))
            this.lastSequence = lastSequence.getOrElse(0)
            this.logNumber = logNumber.getOrElse(0)
            this.preLogNumber = preLogNumber.getOrElse(0)

        } finally {
            try {
                fileChannel.close()
            } catch {
                case _: Throwable => fileChannel.close()
            }
        }

    }

    def pickCompaction(): Option[Compaction] = {
        val sizeCompaction = current.flatMap(version => version.compactScore).exists(score => score > 1)
        val seekCompaction = current.flatMap(version => version.fileToCompact).isDefined

        var level = 0
        var levelInputs = List[FileMetaData]()
        for (version <- current;
             compactLevel <- version.compactLevel) {

            if (sizeCompaction) {
                level = compactLevel
                val fileToAdd = version.getFiles(level).find(file => {
                    !compactPointers.contains(level) || internalKeyComparator.compare(file.largest, compactPointers.get(level).get) > 0
                })
                levelInputs = fileToAdd.getOrElse(version.getFiles(level).head) :: levelInputs
            } else if (seekCompaction) {
                level = version.fileToCompactLevel.get
                levelInputs = List(version.fileToCompact.get)
            } else {
                return Option.empty
            }
        }

        if (level == 0) {
            val (smallest, largest) = getRange(levelInputs)
            levelInputs = getOverlappingInputs(level, smallest.userKey, largest.userKey)
        }

        Option.empty
    }


    def setupOtherInputs(level: Int, levelInputs: List[FileMetaData]): Option[Compaction] = {
        var (smallest, largest) = getRange(levelInputs)
        var newLevelInputs = levelInputs
        var levelUpInputs = getOverlappingInputs(level + 1, smallest.userKey, largest.userKey)

        var (allStart, allEnd) = getRange(levelInputs, levelUpInputs)
        if (levelUpInputs.nonEmpty) {
            val expand0 = getOverlappingInputs(level, allStart.userKey, allEnd.userKey)
            if (expand0.size > levelInputs.size) {
                val (newStart, newEnd) = getRange(expand0)
                val expand1 = getOverlappingInputs(level + 1, newStart.userKey, newEnd.userKey)

                if (expand1.size == levelUpInputs.size) {
                    smallest = newStart
                    largest = newEnd
                    newLevelInputs = expand0
                    levelUpInputs = expand1

                    val (newAllStart, newAllEnd) = getRange(newLevelInputs, levelUpInputs)
                    allStart = newAllStart
                    allEnd = newAllEnd
                }
            }
        }
        var grandparents = List[FileMetaData]()
        if (level + 2 < DbConstants.NUM_LEVELS) {
            grandparents = grandparents ::: getOverlappingInputs(level + 2, allStart.userKey, allEnd.userKey)
        }
        current.flatMap(version => {
            val compaction = Compaction(version, level, newLevelInputs, levelUpInputs, grandparents)
            compactPointers += (level -> largest)
            compaction.edit.setCompactPoint(level, largest)
            Some(compaction)
        })
    }

    def expandRange(left: (InternalKey, InternalKey), right: (InternalKey, InternalKey)): (InternalKey, InternalKey) = {
        val smaller = if (internalKeyComparator.compare(left._1, right._1) < 0) left._1 else right._1
        val larger = if (internalKeyComparator.compare(left._2, right._2) > 0) left._2 else right._2
        (smaller, larger)
    }

    def getRange(inputLists: List[FileMetaData]*): (InternalKey, InternalKey) = {
        require(inputLists.nonEmpty)
        inputLists.map(inputList => {
            inputList.foldLeft((InternalKey.empty, InternalKey.empty)) {
                case (range, file) => expandRange(range, (file.smallest, file.largest))
            }
        }).foldLeft((InternalKey.empty, InternalKey.empty)) {
            case (left, right) => expandRange(left, right)
        }

    }

    def finalizeVersion(version: Version): Unit = {
        var baseLevel = 0
        var baseScore = 1.0 * version.numberOfFilesInLevel(0) / DbConstants.L0_COMPACTION_TRIGGER

        1 to 6 foreach (level => {
            val sumBytes = version.getFiles(level).foldLeft(0L)((levelBytes, file) => levelBytes + file.fileSize)
            val score = 1.0 * sumBytes / maxBytesForLevel(level)
            if (score > baseScore) {
                baseLevel = level
                baseScore = score
            }
        })
        version.compactLevel = Some(baseLevel)
        version.compactScore = Some(baseScore)
    }

    private def maxBytesForLevel(level: Int): Double = {
        val init: Double = 10 * 1048576.0
        init * Math.pow(10, level - 1)
    }
}

case class LevelState(internalKeyComparator: InternalKeyComparator) {
    val fileMetaDataOrdering = VersionSet.getFileMetaDataOrdering(internalKeyComparator)
    var addedFiles = new TreeSet[FileMetaData]()(fileMetaDataOrdering)
    var deletedFiles = HashSet[Long]()

}

case class Builder(versionSet: VersionSet, baseVersion: Version) {
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