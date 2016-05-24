package com.capslock.leveldb

/**
 * Created by capslock.
 */
case class Footer(metaIndexBlockHandle: BlockHandle, indexBlockHandle: BlockHandle)

object Footer {
    val MAGIC_NUMBER = 0xdb4775248b80fb57L
    val MAX_ENCODE_LENGTH = (BlockHandle.MAX_ENCODE_LENGTH * 2) + SizeOf.SIZE_OF_LONG

    def readFooter(slice: Slice): Footer = {
        val sliceInput = SliceInput(slice)
        val metaIndexBlockHandle = BlockHandle.readBlockHandle(sliceInput)
        val indexBlockHandle = BlockHandle.readBlockHandle(sliceInput)

        sliceInput.position = MAX_ENCODE_LENGTH - SizeOf.SIZE_OF_LONG
        val magicNumber = sliceInput.readLong()
        assert(magicNumber == MAGIC_NUMBER, "File is not a table (bad magic number)")
        Footer(metaIndexBlockHandle, indexBlockHandle)
    }

    def writeFooter(footer: Footer, sliceOutput: SliceOutput) = {
        val startIndex = sliceOutput.size
        BlockHandle.writeBlockHandle(footer.metaIndexBlockHandle, sliceOutput)
        BlockHandle.writeBlockHandle(footer.indexBlockHandle, sliceOutput)
        sliceOutput.writeZero(MAX_ENCODE_LENGTH - SizeOf.SIZE_OF_LONG - (sliceOutput.size - startIndex))
        sliceOutput.writeLong(MAGIC_NUMBER)
    }

}
