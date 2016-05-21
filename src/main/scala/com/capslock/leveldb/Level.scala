package com.capslock.leveldb

import com.capslock.leveldb.comparator.InternalKeyComparator


/**
 * Created by capslock.
 */
class Level(levelNumber: Int, tableCache: TableCache, internalKeyComparator: InternalKeyComparator, var files: List[FileMetaData])
    extends SeekingIterable[InternalKey, Slice] {
    val userComparator = internalKeyComparator.userComparator

    def get(lookupKey: LookupKey, readStats: ReadStats): Option[LookupResult] = {
        val index = findFile(lookupKey.internalKey)
        for (targetFile <- files.lift(index);
             iterator <- tableCache.newIterator(targetFile)) yield {
            if (iterator.hasNext) {
                val (key, value) = iterator.next
                if (key.userKey == lookupKey.userKey && key.valueType == ValueType.VALUE) {
                    Some(LookupResult.ok(lookupKey, value))
                } else if (key.userKey == lookupKey.userKey && key.valueType == ValueType.DELETION) {
                    Some(LookupResult.deleted(lookupKey))
                }
            }else{
                Option.empty
            }
        }

        Option.empty
    }

    def addFile(file: FileMetaData): Unit = {
        files = files ::: List(file)
    }

    def someFileOverlapsRange(smallestUserKey: Slice, largestUserKey: Slice): Boolean = {
        val smallestKey = InternalKey(smallestUserKey, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE)
        val index = findFile(smallestKey)
        index < files.size &&
            internalKeyComparator.userComparator.compare(largestUserKey, files(index).smallest.userKey) >= 0
    }

    private def findFile(targetKey: InternalKey): Int = {
        if (files.isEmpty) {
            files.size
        } else {
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
        }
    }

    override def iterator(): SeekingIterator[InternalKey, Slice] = ???
}
