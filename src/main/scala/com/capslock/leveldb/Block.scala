package com.capslock.leveldb

/**
 * Created by capslock.
 */
class Block extends SeekingIterable[Slice, Slice] {
    override def iterator: SeekingIterator[Slice, Slice] = ???

}
