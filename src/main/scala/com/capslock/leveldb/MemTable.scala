package com.capslock.leveldb

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

import com.capslock.leveldb.ValueType.ValueType
import com.capslock.leveldb.comparator.InternalKeyComparator
import com.google.common.collect.Iterators

/**
 * Created by capslock.
 */
class MemTable(internalKeyComparator: InternalKeyComparator) extends SeekingIterable[InternalKey, Slice] {
    val table = new ConcurrentSkipListMap[InternalKey, Slice](internalKeyComparator)
    val _approximateMemoryUsage = new AtomicInteger()

    def isEmpty = table.isEmpty

    def approximateMemoryUsage = _approximateMemoryUsage.get()

    def add(sequenceNumber: Long, valueType: ValueType, key: Slice, value: Slice): Long = {
        val internalKey = InternalKey(key, sequenceNumber, valueType)
        table.put(internalKey, value)
        _approximateMemoryUsage.addAndGet(value.length)
    }

    def get(lookupKey: LookupKey): Option[LookupResult] = {
        val internalKey = lookupKey.internalKey
        val entry = table.ceilingEntry(internalKey)
        if (entry != null) {
            val entryKey = entry.getKey
            if (entryKey == lookupKey.userKey) {
                if (entryKey.valueType == ValueType.DELETION) {
                    Some(LookupResult.ok(lookupKey, entry.getValue))
                } else {
                    Some(LookupResult.deleted(lookupKey))
                }
            } else {
                Option.empty
            }
        } else {
            Option.empty
        }
    }

    override def iterator(): SeekingIterator[InternalKey, Slice] = new MemTableIterator()

    class MemTableIterator extends InternalIterator {
        var iterator = Iterators.peekingIterator(table.entrySet().iterator())

        override def seekToFirst(): Unit = {
            iterator = Iterators.peekingIterator(table.entrySet().iterator())
        }

        override def seek(targetKey: InternalKey): Unit = {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator())
        }

        override def next(): (InternalKey, Slice) = {
            val entry = iterator.next()
            (entry.getKey, entry.getValue)
        }

        override def peek(): (InternalKey, Slice) = {
            val entry = iterator.peek()
            (entry.getKey, entry.getValue)
        }

        override def hasNext: Boolean = iterator.hasNext
    }


}
