package com.capslock.leveldb

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.Comparator

/**
 * Created by capslock.
 */
abstract class Table(name: String, fileChannel: FileChannel, comparator: Comparator[Slice], verifyChecksum: Boolean)
    extends SeekingIterable[Slice, Slice] {


    def init(): Footer

    def readBlock(blockHandle: BlockHandle): Either[Exception, Block]

    def openBlock(blockEntry: Slice): Option[Block] = {
        val blockHandle = BlockHandle.readBlockHandle(SliceInput(blockEntry))
        readBlock(blockHandle) match {
            case Right(block) => Some(block)
            case _ => None
        }
    }

    def uncompressedLength(data: ByteBuffer): Int = {
        VariableLengthQuantity.readVariableLengthInt(data.duplicate())
    }

    def closer: () => Unit = {
        () => {
            fileChannel.close()
        }
    }
    override def iterator(): TableIterator

}
