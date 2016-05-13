package com.capslock.leveldb

import com.google.common.collect.PeekingIterator

/**
 * Created by capslock.
 */
trait SeekingIterator[K, V] extends PeekingIterator[(K, V)] {
    def seekToFirst: Unit

    def seek(targetKey: K): Unit
}
