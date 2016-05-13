package com.capslock.leveldb

import com.capslock.leveldb.BlockIterator.BlockEntry

/**
 * Created by capslock.
 */

class BlockIterator extends SeekingIterator[Slice, Slice] {

    override def seekToFirst: Unit = ???

    override def seek(targetKey: Slice): Unit = ???

    override def next(): BlockEntry = ???

    override def peek(): BlockEntry = ???

    override def hasNext: Boolean = ???
}

object BlockIterator {
    type BlockEntry = (Slice, Slice)
}