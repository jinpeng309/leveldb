package com.capslock.leveldb

import java.nio.channels.FileChannel

import com.capslock.leveldb.LogChunkType.LogChunkType
import com.capslock.leveldb.LogConstants._

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

    def skipToInitialBlock(): Boolean = {
        val offsetInBlock = initialOffset % BLOCK_SIZE
        var blockStartLocation = (initialOffset - offsetInBlock).toInt

        if (offsetInBlock > BLOCK_SIZE - 6) {
            blockStartLocation += BLOCK_SIZE
        }
        endOfBufferOffset = blockStartLocation

        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation)
            } catch {
                case _: Throwable => false
            }

        }
        true
    }


    def readRecord(): Option[Slice] = {
        import LogChunkType._
        recordScratch.reset()

        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return Option.empty[Slice]
            }
        }


        var prospectiveRecordOffset = 0
        var inFragmentRecord = false

        while (true) {
            val physicalRecordOffset = endOfBufferOffset - currentChunk.length
            readNextChunk() match {
                case FULL =>
                    recordScratch.reset()
                    lastRecordOffset = physicalRecordOffset
                    return Option(currentChunk.copySlice())
                case FIRST =>
                    prospectiveRecordOffset = physicalRecordOffset
                    recordScratch.writeBytes(currentChunk)
                    inFragmentRecord = true
                case MIDDLE =>
                    recordScratch.writeBytes(currentChunk)
                case LAST =>
                    recordScratch.writeBytes(currentChunk)
                    lastRecordOffset = prospectiveRecordOffset
                    return Option(recordScratch.slice().copySlice())
                case EOF =>
                    recordScratch.reset()
                    return Option.empty
                case _ =>
                    recordScratch.reset()
                    return Option.empty
            }
        }
        Option.empty
    }


    private def readNextBlock(): Boolean = {
        if (eof) {
            return false
        }

        blockScratch.reset()

        while (blockScratch.isWritable) {
            try {
                val bytesRead = blockScratch.writeBytes(fileChannel, blockScratch.writableBytes)
                if (bytesRead < 0) {
                    eof = true
                    currentBlock = SliceInput(blockScratch.slice())
                    return currentBlock.isReadable
                }
                endOfBufferOffset += bytesRead
            } catch {
                case _: Throwable =>
                    currentBlock = SliceInput(Slice(0))
                    eof = true
                    return false
            }
        }
        currentBlock = SliceInput(blockScratch.slice())
        currentBlock.isReadable
    }

    private def readNextChunk(): LogChunkType = {
        currentChunk = Slice(0)

        if (currentBlock.available < HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return LogChunkType.EOF
                }
            }
        }

        val expectedChecksum = currentBlock.readInt
        var length = currentBlock.readUnsignedByte()
        length = length | (currentBlock.readUnsignedByte() << 8)
        val chunkTypeId = currentBlock.readByte
        val logChunkType = LogChunkType(chunkTypeId)

        if (length > currentBlock.available) {
            currentBlock = SliceInput(Slice(0))
            return LogChunkType.BAD_CHUNK
        }

        currentChunk = currentBlock.readBytes(length)

        if (verifyChecksum) {
            val actualCheckSum = LogChunkType.getChunkChecksum(logChunkType, currentChunk)
            if (actualCheckSum != expectedChecksum) {
                currentBlock = SliceInput(Slice(0))
                return LogChunkType.BAD_CHUNK
            }
        }

        logChunkType
    }
}

object LogReader {
    def apply(fileChannel: FileChannel, verifyChecksum: Boolean, initialOffset: Long): LogReader = {
        new LogReader(fileChannel, verifyChecksum, initialOffset)
    }
}
