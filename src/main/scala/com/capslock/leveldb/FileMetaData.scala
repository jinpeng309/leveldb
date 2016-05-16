package com.capslock.leveldb

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by capslock.
 */
case class FileMetaData(fileNumber: Long, fileSize: Long, smallest: InternalKey, largest: InternalKey) {
    var allowedSeeks = new AtomicInteger(1 << 30)

    def allowedSeeks_=(value: Int) = allowedSeeks.set(value)

    def decrementAllowedSeeks(): Int = allowedSeeks.getAndDecrement()
}
