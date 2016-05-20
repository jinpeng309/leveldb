package com.capslock.leveldb

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
 * Created by capslock.
 */
class Version(val versionSet: VersionSet) {
    val retained = new AtomicInteger(1)

    def getFiles(): mutable.HashMap[Int, List[FileMetaData]] with mutable.MultiMap[Int, FileMetaData] = ???

    def retain(): Unit = {
        val was = retained.getAndIncrement()
        require(was > 0, "Version was retain after it was disposed.")
    }

    def release(): Unit = {
        val was = retained.getAndDecrement()
        require(was >= 0, "Version was released after it was disposed.")
    }
}
