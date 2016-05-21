package com.capslock.leveldb

import com.capslock.leveldb.comparator.InternalKeyComparator

/**
 * Created by capslock.
 */
class Level0(var files: List[FileMetaData], tableCache: TableCache, internalKeyComparator: InternalKeyComparator)
    extends SeekingIterable[InternalKey, Slice] {

    def addFile(file: FileMetaData): Unit = {
        files = files ::: List(file)
    }

    def someFileOverlapsRange(smallestUserKey: Slice, largestUserKey: Slice): Boolean = {
        val smallestInternalKey = InternalKey(smallestUserKey, SequenceNumber.MAX_SEQUENCE_NUMBER, ValueType.VALUE)
        val index = findFile(smallestInternalKey)
        val userComparator = internalKeyComparator.userComparator
        index < files.size && userComparator.compare(largestUserKey, files(index).smallest.userKey) >= 0
    }

    def get(targetKey: LookupKey, readStats: ReadStats): Option[LookupResult] = {
        if (files.nonEmpty) {
            readStats.reset()

            files
                .filter(file => {
                    val userComparator = internalKeyComparator.userComparator
                    (userComparator.compare(targetKey.userKey, file.smallest.userKey) >= 0) &&
                        (userComparator.compare(targetKey.userKey, file.largest.userKey) <= 0)
                })
                .sortWith((left, right) => right.fileNumber - left.fileNumber < 0)
                .foreach(file => {
                    for (iterator <- tableCache.newIterator(file)) {
                        iterator.seek(targetKey.internalKey)
                        if (iterator.hasNext) {
                            val (key, value) = iterator.next
                            if (key == targetKey.internalKey && key.valueType == ValueType.VALUE) {
                                return Some(LookupResult.ok(targetKey, value))
                            } else if (key == targetKey.internalKey && key.valueType == ValueType.DELETION) {
                                return Some(LookupResult.deleted(targetKey))
                            }
                        }
                        if (readStats.seekFile.isEmpty) {
                            readStats.seekFile = file
                            readStats.seekFileLevel = 0
                        }
                    }
                })

            Option.empty
        } else {
            Option.empty
        }
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
            files.size
        }
    }

    override def iterator(): InternalIterator = Level0Iterator(tableCache, files, internalKeyComparator)
}
