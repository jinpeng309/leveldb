package com.capslock.leveldb

import java.nio.channels.FileChannel

/**
 * Created by capslock.
 */
class LogReader(val fileChannel: FileChannel, verifyChecksum: Boolean, initialOffset: Long) {
    val recordScratch = DynamicSliceOutput(LogConstants.BLOCK_SIZE)
    val blockScratch = BasicSliceOutput(Slice(LogConstants.BLOCK_SIZE))
    var eof = false
    var lastRecordOffset = 0
    var endOfBufferOffset = 0
    var currentBlock = SliceInput(Slice(0))
    var currentChunk = Slice(0)


}
