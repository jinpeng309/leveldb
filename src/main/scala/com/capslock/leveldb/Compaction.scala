package com.capslock.leveldb

/**
 * Created by capslock.
 */
case class Compaction(inputVersion: Version, level: Int, levelInputs: List[FileMetaData],
                 levelUpInputs: List[FileMetaData], grandparents: List[FileMetaData]) {
    val edit = VersionEdit()
}
