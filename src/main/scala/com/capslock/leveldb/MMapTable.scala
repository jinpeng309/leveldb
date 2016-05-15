package com.capslock.leveldb

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder, MappedByteBuffer}
import java.util.Comparator

/**
 * Created by capslock.
 */
class MMapTable(name: String, fileChannel: FileChannel, comparator: Comparator[Slice], verifyChecksum: Boolean)
    extends Table(name, fileChannel, comparator, verifyChecksum) {
    val data: MappedByteBuffer = {
        val size = fileChannel.size()
        fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size)
    }
    val footer = init()
    val indexBlock = readBlock(footer.indexBlockHandle) match {
        case Right(block) => Some(block)
        case _ => None
    }
    val metaIndexBlockHandle = footer.metaIndexBlockHandle


    override def init(): Footer = {
        val size = fileChannel.size()
        Footer.readFooter(Slice.copiedBuffer(data, (size - Footer.MAX_ENCODE_LENGTH).toInt, Footer.MAX_ENCODE_LENGTH))
    }

    override def closer: () => Unit = {
        () => {
            val unmapMethod = fileChannel.getClass.getDeclaredMethod("unmap", classOf[MappedByteBuffer])
            unmapMethod.setAccessible(true)
            unmapMethod.invoke(null, data)
            fileChannel.close()
        }
    }

    override def readBlock(blockHandle: BlockHandle): Either[Exception, Block] = {
        val blockTrailer = BlockTrailer.readBlockTrailer(Slice.copiedBuffer(data.asReadOnlyBuffer(),
            (blockHandle.offset + blockHandle.dataSize).toInt, BlockTrailer.ENCODE_LENGTH))
        try {
            val unCompressedBuffer = read(data, blockHandle.offset.toInt, blockHandle.dataSize.toInt)
            if (blockTrailer.compressionType == CompressionType.SNAPPY) {
                //todo uncompress process
                val uncompressedData = Slice.copiedBuffer(unCompressedBuffer)
                Right(Block(uncompressedData, comparator))
            } else {
                val uncompressedData = Slice.copiedBuffer(unCompressedBuffer)
                Right(Block(uncompressedData, comparator))
            }
        } catch {
            case ex: IOException => Left(ex)
        }


    }

    @throws(classOf[IOException])
    def read(data: MappedByteBuffer, offset: Int, length: Int): ByteBuffer = {
        val newPosition: Int = data.position + offset
        data.duplicate.order(ByteOrder.LITTLE_ENDIAN).clear.limit(newPosition + length).position(newPosition).asInstanceOf[ByteBuffer]
    }

    @throws(classOf[IllegalStateException])
    override def iterator(): TableIterator = {
        indexBlock match {
            case Some(block) => TableIterator(this, indexBlock.get.iterator())
            case _ => throw new IllegalStateException("Empty index block, may some err in reading file")
        }
    }
}
