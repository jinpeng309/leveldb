package com.capslock.leveldb

import com.capslock.leveldb.SizeOf._

/**
 * Created by capslock.
 */
object LogConstants {
    val BLOCK_SIZE = 32768
    val HEADER_SIZE = SIZE_OF_INT + SIZE_OF_BYTE + SIZE_OF_SHORT
}
