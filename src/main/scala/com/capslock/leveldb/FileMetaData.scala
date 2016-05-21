package com.capslock.leveldb

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by capslock.
 */
case class FileMetaData(fileNumber: Long, fileSize: Long, smallest: InternalKey, largest: InternalKey) {
    var _allowedSeeks = new AtomicInteger(1 << 30)

    def allowedSeeks = _allowedSeeks.get()

    def allowedSeeks_=(value: Int): Unit = _allowedSeeks.set(value)

    def decrementAllowedSeeks(): Int = _allowedSeeks.getAndDecrement()
}
