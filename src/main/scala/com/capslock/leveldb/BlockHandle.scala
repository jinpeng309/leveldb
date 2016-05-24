package com.capslock.leveldb

/**
 * Created by capslock.
 */
case class BlockHandle(offset: Long, dataSize: Long)

object BlockHandle {
    val MAX_ENCODE_LENGTH = 10 + 10

    def readBlockHandle(sliceInput: SliceInput): BlockHandle = {
        val offset = VariableLengthQuantity.readVariableLengthLong(sliceInput)
        val size = VariableLengthQuantity.readVariableLengthLong(sliceInput)
        BlockHandle(offset, size)
    }

    def writeBlockHandle(blockHandler: BlockHandle): Slice = {
        val slice = Slice(MAX_ENCODE_LENGTH)
        val sliceOutput = BasicSliceOutput(slice)
        writeBlockHandle(blockHandler, sliceOutput)
        slice
    }

    def writeBlockHandle(blockHandler: BlockHandle, sliceOutput: SliceOutput): Unit = {
        VariableLengthQuantity.writeVariableLengthLong(blockHandler.offset, sliceOutput)
        VariableLengthQuantity.writeVariableLengthLong(blockHandler.dataSize, sliceOutput)
    }
}
