package com.capslock.leveldb

import java.io.{File, IOException}

/**
 * Created by capslock.
 */
trait LogWriter {
    def isClosed: Boolean

    @throws(classOf[IOException])
    def close()

    @throws(classOf[IOException])
    def delete()

    def getFile: File

    def getFileNumber: Long

    @throws(classOf[IOException])
    def addRecord(record: Slice, force: Boolean)
}
