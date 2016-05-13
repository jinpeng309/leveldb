package com.capslock.leveldb.comparator

import java.util.Comparator

import com.capslock.leveldb.Slice

/**
 * Created by capslock.
 */
trait UserComparator extends Comparator[Slice] {
    def name: String

    def findShortestSeparator(start: Slice, limit: Slice): Slice

    def findShortSuccessor(key: Slice): Slice
}
