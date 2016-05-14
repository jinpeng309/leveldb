package com.capslock.leveldb

import java.nio.channels.FileChannel
import java.util.Comparator

/**
 * Created by capslock.
 */
abstract class Table(name: String, fileChannel: FileChannel, comparator: Comparator[Slice], verifyChecksum: Boolean)
    extends SeekingIterable[Slice, Slice] {
    val footer = init()
    val indexBlock = readBlock(footer.indexBlockHandle) match {
        case Right(block) => Some(block)
        case _ => None
    }
    val metaIndexBlockHandle = footer.metaIndexBlockHandle

    def init(): Footer

    def readBlock(blockHandle: BlockHandle): Either[Exception, Block]

    def openBlock(blockEntry: Slice): Option[Block] = {
        val blockHandle = BlockHandle.readBlockHandle(SliceInput(blockEntry))
        readBlock(blockHandle) match {
            case Right(block) => Some(block)
            case _ => None
        }
    }

    def closer: () => Unit = {
        () => {
            fileChannel.close()
        }
    }
}
