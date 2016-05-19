package com.capslock.leveldb.comparator

import java.util.Comparator

import com.capslock.leveldb.InternalKey
import com.google.common.primitives.Longs

/**
 * Created by capslock.
 */
class InternalKeyComparator(userComparator: UserComparator) extends Comparator[InternalKey] {
    override def compare(leftKey: InternalKey, rightKey: InternalKey): Int = {
        val result = userComparator.compare(leftKey.userKey, rightKey.userKey)
        if (result != 0) {
            return result
        }
        Longs.compare(rightKey.sequenceNumber, leftKey.sequenceNumber)
    }

    def name(): String = {
        userComparator.name
    }
}

object InternalKeyComparator {
    def apply(userComparator: UserComparator): InternalKeyComparator = {
        new InternalKeyComparator(userComparator)
    }
}
