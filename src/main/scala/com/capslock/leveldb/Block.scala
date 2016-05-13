package com.capslock.leveldb

import java.util.Comparator

/**
 * Created by capslock.
 */
class Block extends SeekingIterable[Slice, Slice] {
    override def iterator(): SeekingIterator[Slice, Slice] = BlockIterator(Slice(3), Slice(3), new Comparator[Slice] {
        override def compare(o1: Slice, o2: Slice): Int = 1
    } )
}
