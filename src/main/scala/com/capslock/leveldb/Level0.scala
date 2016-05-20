package com.capslock.leveldb

import com.capslock.leveldb.comparator.InternalKeyComparator

/**
 * Created by capslock.
 */
class Level0(var files: List[FileMetaData], tableCache: TableCache, internalKeyComparator: InternalKeyComparator)
    extends SeekingIterable[InternalKey, Slice] {

    def addFile(file: FileMetaData): Unit = {
        files = file :: files
    }

    def someFileOverlapsRange(smallestUserKey: Slice, largestUserKey: Slice): Boolean = {
        val smallestInternalKey = InternalKey(smallestUserKey, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE)
        val index = findFile(smallestInternalKey)
        val userComparator = internalKeyComparator.userComparator
        index < files.size && userComparator.compare(largestUserKey, files(index).smallest.userKey) >= 0
    }

    def get(key: LookupKey, readStats: ReadStats): Option[LookupResult] = {

        Option.empty
    }

    private def findFile(targetKey: InternalKey): Int = {
        if (files.nonEmpty) {
            var left = 0
            var right = files.size - 1

            while (left < right) {
                val mid = (left + right) / 2
                if (internalKeyComparator.compare(files(mid).largest, targetKey) < 0) {
                    left = mid + 1
                } else {
                    right = mid
                }
            }
            right
        } else {
            0
        }
    }

    override def iterator(): SeekingIterator[InternalKey, Slice] = ???
}
