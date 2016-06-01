package com.capslock.leveldb

import java.io.{File, IOException, RandomAccessFile}
import java.nio.channels.FileLock

/**
 * Created by capslock.
 */
class DbLock(lockFile: File) {
    val fileChannel = new RandomAccessFile(lockFile, "rw").getChannel
    var fileLock = Option.empty[FileLock]
    try {
        fileLock = Some(fileChannel.tryLock())
    } catch {
        case e: IOException => Closeables.closeQuietly(fileChannel); throw e
    }
    if (fileLock.isEmpty) {
        throw new IOException(s"unable to acquire lock file ${lockFile.getAbsolutePath}")
    }

    def release(): Unit = {
        for (lock <- fileLock) {
            lock.release()
        }
    }
}

object DbLock {
    @throws[IOException]
    def apply(lockFile: File): DbLock = {
        new DbLock(lockFile)
    }
}
