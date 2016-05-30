package com.capslock.leveldb

/**
 * Created by capslock.
 */
class Options {
    var createIfMissing = true
    var errorIfExists = false
    var writeBufferSize = 4 << 20
    var maxOpenFiles = 1000
    var blockRestartInterval = 16
    var blockSize = 4 * 1024
    var compressType = CompressionType.NONE
    var verifyChecksum = true
}

object Options {
    def apply(): Options = {
        new Options()
    }
}
