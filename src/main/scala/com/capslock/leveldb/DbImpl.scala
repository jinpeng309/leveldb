package com.capslock.leveldb

import java.io.File

import com.capslock.leveldb.comparator.{BytewiseComparator, InternalKeyComparator}

/**
 * Created by capslock.
 */
class DbImpl(options: Options, databaseDir: File) {
    val internalKeyComparator = InternalKeyComparator(BytewiseComparator())
}
