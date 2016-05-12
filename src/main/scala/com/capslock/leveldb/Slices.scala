package com.capslock.leveldb

/**
 * Created by capslock.
 */
object Slices {
    def ensureSize(slice: Slice, minWritableSize: Int): Slice = {
        slice.length match {
            case _ if slice.length >= minWritableSize => slice
            case _ if slice.length < minWritableSize =>
                val minCapability = slice.length + minWritableSize
                var newCapability = if (slice.length == 0) 1 else slice.length
                while (newCapability < minCapability) {
                    newCapability <<= 1
                }
                val result = Slice(newCapability)
                result.setBytes(0, slice, 0, slice.length)
                result
        }
    }
}
