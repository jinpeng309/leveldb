package com.capslock.leveldb

/**
 * Created by capslock.
 */
trait SeekingIterable[K, V]{
    def iterator: SeekingIterator[K, V]
}
