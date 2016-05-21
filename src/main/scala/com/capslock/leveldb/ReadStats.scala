package com.capslock.leveldb

/**
 * Created by capslock.
 */
class ReadStats(private var _seekFileLevel: Option[Int] = Option.empty,
                private var _seekFile: Option[FileMetaData] = Option.empty) {

    def reset(): Unit = {
        _seekFile = Option.empty
        _seekFileLevel = Option.empty
    }

    def seekFileLevel_=(level: Int): Unit = {
        _seekFileLevel = Option(level)
    }

    def seekFile_=(fileMetaData: FileMetaData): Unit = {
        _seekFile = Option(fileMetaData)
    }

    def seekFile = _seekFile

    def seekFileLevel = _seekFileLevel
}

object ReadStats {
    def apply(level: Int, fileMetaData: FileMetaData): ReadStats = {
        new ReadStats(Some(level), Some(fileMetaData))
    }

    def apply():ReadStats={
        new ReadStats()
    }
}