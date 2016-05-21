package com.capslock.leveldb

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by capslock.
 */
class Version(val versionSet: VersionSet) {
    val retained = new AtomicInteger(1)
    val level0 = new Level0(List(), versionSet.tableCache, versionSet.internalKeyComparator)
    val levels = List.tabulate(DbConstants.NUM_LEVELS - 1)(level =>
        new Level(level + 1, versionSet.tableCache, versionSet.internalKeyComparator, List()))

    def numberOfFilesInLevel(level: Int): Int = {
        level match {
            case 0 => level0.files.size
            case _ if level <= DbConstants.NUM_LEVELS => levels(level - 1).files.size
        }
    }

    def getLevel0Files(): List[InternalTableIterator] = {
        level0.files.flatMap(file => versionSet.tableCache.newIterator(file))
    }

    def getLevelIterators(): List[LevelIterator] = {
        levels.filter(level => level.files.nonEmpty).map(level => level.iterator())
    }

    def getFiles(): Map[Int, List[FileMetaData]] = {
        var result = Map[Int, List[FileMetaData]]()
        result += (0 -> level0.files)
        levels.foreach(level => result += (level.levelNumber -> level.files))
        result
    }

    def getFiles(level: Int): List[FileMetaData] = {
        level match {
            case 0 => level0.files
            case _ if level < DbConstants.NUM_LEVELS => levels(level - 1).files
            case _ => List()
        }
    }

    def totalFileSize(files: List[FileMetaData]): Long = {
        files.foldLeft(0: Long)((size, file) => size + file.fileSize)
    }

    def pickLevelForMemTableOutput(smallest: Slice, largest: Slice): Int = {
        var level = 0
        if (!overlapInLevel(0, smallest, largest)) {

            while (level < DbConstants.MAX_MEM_COMPACT_LEVEL) {
                if (overlapInLevel(level + 1, smallest, largest)) {
                    return level
                }

                val sum = totalFileSize(versionSet.getOverlappingInputs(level + 2, smallest, largest))
                if (sum > VersionSet.MAX_GRAND_PARENT_OVERLAP_BYTES) {
                    return level
                }
                level += 1
            }
        }
        level
    }

    def overlapInLevel(level: Int, smallest: Slice, largest: Slice): Boolean = {
        level match {
            case 0 => level0.someFileOverlapsRange(smallest, largest)
            case _ if level < DbConstants.NUM_LEVELS => levels(level - 1).someFileOverlapsRange(smallest, largest)
        }
    }

    def getInLevel0(lookupKey: LookupKey, readStats: ReadStats): Option[LookupResult] = {
        level0.get(lookupKey, readStats)
    }

    def getInLevel(lookupKey: LookupKey, readStats: ReadStats): Option[LookupResult] = {
        levels.foreach(level => {
            val result = level.get(lookupKey, readStats)
            if (result.isDefined) {
                return result
            }
        })
        Option.empty
    }

    def get(lookupKey: LookupKey): Option[LookupResult] = {
        val readStats = ReadStats()
        getInLevel0(lookupKey, readStats).orElse(getInLevel0(lookupKey, readStats))
    }

    def retain(): Unit = {
        val was = retained.getAndIncrement()
        require(was > 0, "Version was retain after it was disposed.")
    }

    def release(): Unit = {
        val was = retained.getAndDecrement()
        require(was >= 0, "Version was released after it was disposed.")
    }
}
