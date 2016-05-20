package com.capslock.leveldb

import java.util.Comparator

/**
 * Created by capslock.
 */
class Level0Iterator(val tableCache: TableCache, val files: List[FileMetaData], val comparator: Comparator[InternalKey])
    extends AbstractSeekingIterator[InternalKey, Slice] with InternalIterator {

    override protected def seekToFirstInternal(): Unit = ???

    override protected def seekInternal(targetKey: InternalKey): Unit = ???

    override protected def getNextElement(): Option[(InternalKey, Slice)] = ???
}

object Level0Iterator {
    def apply(tableCache: TableCache, files: List[FileMetaData], comparator: Comparator[InternalKey]): Level0Iterator = {
        new Level0Iterator(tableCache, files, comparator)
    }
}
