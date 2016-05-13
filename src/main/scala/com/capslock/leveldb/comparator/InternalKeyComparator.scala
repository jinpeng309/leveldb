package com.capslock.leveldb.comparator

import java.util.Comparator

import com.capslock.leveldb.InternalKey

/**
 * Created by capslock.
 */
class InternalKeyComparator(userComparator: UserComparator) extends Comparator[InternalKey] {
    override def compare(leftKey: InternalKey, rightKey: InternalKey): Int = {
        import scala.math.Ordered.orderingToOrdered
        (leftKey.userKey, rightKey.sequenceNumber) compareTo(rightKey.userKey, leftKey.sequenceNumber)
    }
}

object InternalKeyComparator {
    def apply(userComparator: UserComparator): InternalKeyComparator = {
        new InternalKeyComparator(userComparator)
    }
}
