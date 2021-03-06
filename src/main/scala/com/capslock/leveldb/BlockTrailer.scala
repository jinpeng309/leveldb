package com.capslock.leveldb

import com.capslock.leveldb.CompressionType.CompressionType

/**
 * Created by capslock.
 */
case class BlockTrailer(crc32c: Int, compressionType: CompressionType)

object BlockTrailer {
    val ENCODE_LENGTH = 5
    def readBlockTrailer(slice: Slice): BlockTrailer = {
        val sliceInput = SliceInput(slice)
        val compressionType = CompressionType(sliceInput.readUnsignedByte())
        val crc32c = sliceInput.readInt
        BlockTrailer(crc32c, compressionType)
    }

    def writeBlockTrailer(blockTrailer: BlockTrailer): Slice = {
        val slice = Slice(ENCODE_LENGTH)
        writeBlockTrailer(blockTrailer, BasicSliceOutput(slice))
        slice
    }

    def writeBlockTrailer(blockTrailer: BlockTrailer, sliceOutput: SliceOutput) = {
        sliceOutput.writeByte(blockTrailer.compressionType.id)
        sliceOutput.writeInt(blockTrailer.crc32c)
    }
}