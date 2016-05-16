package com.capslock.leveldb

/**
 * Created by alvin.
 */
class InternalTableIterator(tableIterator: TableIterator) extends AbstractSeekingIterator[InternalKey, Slice] with InternalIterator {
    override protected def seekToFirstInternal(): Unit = tableIterator.seekToFirst()

    override protected def seekInternal(targetKey: InternalKey): Unit = tableIterator.seek(targetKey.toSlice)

    override protected def getNextElement(): Option[(InternalKey, Slice)] = {
        import InternalKey.SliceToInternalKeyImplicit
        if (tableIterator.hasNext) {
            val (key, value) = tableIterator.next
            Some((key.toInternalKey, value))
        } else {
            Option.empty
        }
    }
}

object InternalTableIterator{
    def apply(tableIterator: TableIterator):InternalTableIterator = {
        new InternalTableIterator(tableIterator)
    }
}
