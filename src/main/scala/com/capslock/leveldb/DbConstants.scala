package com.capslock.leveldb

/**
 * Created by capslock.
 */
object DbConstants {
    val MAJOR_VERSION: Int = 0
    val MINOR_VERSION: Int = 1
    // todo this should be part of the configuration
    /**
     * Max number of levels
     */
    val NUM_LEVELS: Int = 7
    /**
     * Level-0 compaction is started when we hit this many files.
     */
    val L0_COMPACTION_TRIGGER: Int = 4
    /**
     * Soft limit on number of level-0 files.  We slow down writes at this point.
     */
    val L0_SLOWDOWN_WRITES_TRIGGER: Int = 8
    /**
     * Maximum number of level-0 files.  We stop writes at this point.
     */
    val L0_STOP_WRITES_TRIGGER: Int = 12
    /**
     * Maximum level to which a new compacted memtable is pushed if it
     * does not create overlap.  We try to push to level 2 to avoid the
     * relatively expensive level 0=>1 compactions and to avoid some
     * expensive manifest file operations.  We do not push all the way to
     * the largest level since that can generate a lot of wasted disk
     * space if the same key space is being repeatedly overwritten.
     */
    val MAX_MEM_COMPACT_LEVEL: Int = 2
}
