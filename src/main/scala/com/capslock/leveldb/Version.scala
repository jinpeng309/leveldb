package com.capslock.leveldb

import scala.collection.mutable

/**
 * Created by capslock.
 */
class Version(val versionSet: VersionSet) {
    def getFiles():mutable.HashMap[Int, List[FileMetaData]] with mutable.MultiMap[Int, FileMetaData] = ???
}
