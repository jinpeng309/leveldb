package com.capslock.leveldb

import com.capslock.leveldb.comparator.InternalKeyComparator

/**
 * Created by capslock.
 */
case class LevelIterator(tableCache: TableCache, files: List[FileMetaData], internalKeyComparator: InternalKeyComparator)
    extends AbstractSeekingIterator[InternalKey, Slice] with InternalIterator {
    var index = 0
    var current = Option.empty[InternalTableIterator]

    private def openNextFile(): Option[InternalTableIterator] = {
        index += 1
        files.lift(index - 1).flatMap(file => tableCache.newIterator(file))
    }

    override protected def seekToFirstInternal(): Unit = {
        index = 0
        current = Option.empty
    }

    override protected def seekInternal(targetKey: InternalKey): Unit = {
        var left: Int = 0
        var right: Int = files.size - 1

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            val mid: Int = (left + right) / 2
            if (internalKeyComparator.compare(files(mid).largest, targetKey) < 0) {
                left = mid + 1
            }
            else {
                right = mid
            }
        }
        index = right

        if (index == files.size - 1 && internalKeyComparator.compare(files(index).largest, targetKey) < 0) {
            index += 1
        }

        if (index < files.size) {
            current = openNextFile()
            for (iterator <- current) {
                iterator.seek(targetKey)
            }
        } else {
            current = Option.empty
        }

    }

    override protected def getNextElement(): Option[(InternalKey, Slice)] = {
        var currentHasNext = false

        while (true) {
            for (iterator <- current) {
                currentHasNext = iterator.hasNext
            }
            if (!currentHasNext) {
                if (index < files.size) {
                    current = openNextFile()
                } else {
                    current = Option.empty
                    return Option.empty
                }
            } else {
                return current.flatMap(iterator => Some(iterator.next))
            }
        }
        Option.empty
    }


}
