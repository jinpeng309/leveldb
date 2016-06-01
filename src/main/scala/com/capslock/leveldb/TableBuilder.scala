package com.capslock.leveldb

import java.nio.channels.FileChannel
import java.io.IOException
import com.capslock.leveldb.CompressionType.CompressionType
import com.capslock.leveldb.comparator.{BytewiseComparator, UserComparator}


/**
 * Created by capslock.
 */
class TableBuilder(options: Options, fileChannel: FileChannel, userComparator: UserComparator) {
    val blockRestartInterval = options.blockRestartInterval
    val blockSize = options.blockSize
    val compressType = options.compressType
    val dataBlockBuilder = BlockBuilder(Math.min(blockSize * 1.1, VersionSet.TARGET_FILE_SIZE).toInt,
        blockRestartInterval, userComparator)
    val indexBlockBuilder = BlockBuilder(BlockHandle.MAX_ENCODE_LENGTH * 1024, 1, userComparator)
    var lastKey = Slice.empty
    var entryCount = 0
    var position = 0
    var pendingIndexEntry = false
    var pendingHandle = Option.empty[BlockHandle]

    private def maxCompressedLength(length: Int): Int = {
        32 + length + (length / 6)
    }

    private def flush(): Unit = {
        pendingHandle = Some(writeBlock(dataBlockBuilder))
        pendingIndexEntry = true
    }

    private def writeBlock(blockBuilder: BlockBuilder): BlockHandle = {
        val raw = blockBuilder.finish()
        val blockTrailer = BlockTrailer(crc32(raw, CompressionType.NONE), CompressionType.NONE)
        val trailer = BlockTrailer.writeBlockTrailer(blockTrailer)
        val blockHandle = BlockHandle(position, raw.length)
        position += fileChannel.write(Array(raw.toByteBuffer(), trailer.toByteBuffer())).toInt
        blockBuilder.reset()
        blockHandle
    }

    private def crc32(data: Slice, compressionType: CompressionType): Int = {
        val crc32 = new PureJavaCrc32C()
        crc32.update(data.data, data.offset, data.length)
        crc32.update(compressType.id & 0xFF)
        crc32.getMaskedValue
    }

    @throws(classOf[IOException])
    def add(key: Slice, value: Slice): Unit = {
        if (entryCount > 0) {
            require(userComparator.compare(lastKey, key) > 0, "key must be greater than last key")
        }

        if (pendingIndexEntry) {
            require(pendingHandle.isDefined)
            val shortestSeparator = userComparator.findShortestSeparator(lastKey, key)
            for (handle <- pendingHandle) {
                val handleEncoding = BlockHandle.writeBlockHandle(handle)
                indexBlockBuilder.add(shortestSeparator, handleEncoding)
                pendingIndexEntry = false
            }
        }
        lastKey = key
        entryCount += 1
        dataBlockBuilder.add(key, value)
        if (dataBlockBuilder.estimate >= blockSize) {
            flush()
        }
    }

    def finish(): Unit = {
        flush()
        // write (empty) meta index block
        val metaIndexBlockBuilder = BlockBuilder(256, blockRestartInterval, BytewiseComparator())
        val metaIndexBlockHandle = writeBlock(metaIndexBlockBuilder)

        if (pendingIndexEntry) {
            val shortestSeparator = userComparator.findShortSuccessor(lastKey)
            for (handle <- pendingHandle) {
                val handleEncoding = BlockHandle.writeBlockHandle(handle)
                indexBlockBuilder.add(shortestSeparator, handleEncoding)
                pendingIndexEntry = false
            }
        }
        val indexBlockHandle = writeBlock(indexBlockBuilder)
        val footer = Footer(metaIndexBlockHandle, indexBlockHandle)
        val sliceOutput = DynamicSliceOutput(Footer.MAX_ENCODE_LENGTH.toInt)
        Footer.writeFooter(footer, sliceOutput)
        position += fileChannel.write(sliceOutput.slice().toByteBuffer())
    }
}

object TableBuilder {
    def apply(options: Options, fileChannel: FileChannel, userComparator: UserComparator): TableBuilder = {
        new TableBuilder(options, fileChannel, userComparator)
    }
}
