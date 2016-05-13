package com.capslock.leveldb

/**
 * Created by capslock.
 */
trait SeekingIterable[K, V] extends Iterable[(K, V)] {
    override def iterator: SeekingIterator[K, V]
}
