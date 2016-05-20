package com.capslock.leveldb

import java.io.{File, IOException, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.atomic.AtomicBoolean

import com.capslock.leveldb.LogChunkType._
import com.capslock.leveldb.LogConstants._

/**
 * Created by capslock.
 */
class MMapLogWriter(file: File, fileNumber: Long) extends LogWriter {
    val fileChannel = new RandomAccessFile(file, "rw").getChannel
    var mappedByteBuffer = Option(fileChannel.map(MapMode.READ_WRITE, 0, MMapLogWriter.PAGE_SIZE))
    val closed = new AtomicBoolean()
    var fileOffset = 0
    var blockOffset = 0

    override def isClosed: Boolean = closed.get()


    @throws(classOf[IOException])
    override def addRecord(record: Slice, force: Boolean): Unit = {
        require(!closed.get(), "Log has been closed")

        val sliceInput = SliceInput(record)
        var begin = true
        do {
            var bytesRemainingInBlock = BLOCK_SIZE - blockOffset

            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    ensureCapacity(bytesRemainingInBlock)
                    mappedByteBuffer.get.put(Array.fill[Byte](bytesRemainingInBlock)(0))
                }
                blockOffset = 0
                bytesRemainingInBlock = BLOCK_SIZE
            }
            val bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE
            val end = if (sliceInput.available > bytesAvailableInBlock) false else true
            val fragmentLength = {
                if (sliceInput.available > bytesAvailableInBlock) bytesAvailableInBlock else sliceInput.available
            }
            val logChunkType = (begin, end) match {
                case (true, true) => FULL
                case (true, false) => FIRST
                case (false, true) => LAST
                case (false, false) => MIDDLE
            }
            writeChunk(logChunkType, sliceInput.readBytes(fragmentLength))
            begin = false

        } while (sliceInput.isReadable)


        for (buffer <- mappedByteBuffer) {
            if (force)
                buffer.force()
        }
    }

    def writeChunk(logChunkType: LogChunkType, slice: Slice) = {
        require(!closed.get(), "Log has been closed")
        val header = newLogRecordHeader(logChunkType, slice)
        val buffer = mappedByteBuffer.get
        ensureCapacity(HEADER_SIZE + slice.length)
        header.getBytes(0, buffer)
        slice.getBytes(0, buffer)

        blockOffset += HEADER_SIZE + slice.length
    }


    def newLogRecordHeader(logChunkType: LogChunkType, slice: Slice): Slice = {
        val crc = getChunkChecksum(logChunkType, slice)
        val header = Slice(HEADER_SIZE)
        val sliceOutput = BasicSliceOutput(header)
        sliceOutput.writeInt(crc)
        sliceOutput.writeByte((slice.length & 0xff).toByte)
        sliceOutput.writeByte((slice.length >>> 8).toByte)
        sliceOutput.writeByte(logChunkType.id.toByte)
        header
    }

    def ensureCapacity(bytes: Int) = {
        for (buffer <- mappedByteBuffer; if buffer.remaining() > bytes) {
            fileOffset += buffer.position()
            unmap()
            mappedByteBuffer = Option(fileChannel.map(MapMode.READ_WRITE, fileOffset, MMapLogWriter.PAGE_SIZE))
        }
    }

    @throws(classOf[IOException])
    override def delete(): Unit = {
        close()
        file.delete()
    }

    override def getFileNumber: Long = fileNumber

    @throws(classOf[IOException])
    override def close(): Unit = {
        if (!closed.get()) {
            closed.set(true)
            if (fileChannel.isOpen) {
                fileChannel.truncate(fileOffset)
            }
            destroyMappedByteBuffer()
            Closeables.closeQuietly(fileChannel)
        }
    }

    private def destroyMappedByteBuffer(): Unit = {
        mappedByteBuffer.foreach(buffer => {
            fileOffset += buffer.position()
            unmap()
        })
        mappedByteBuffer = Option.empty[MappedByteBuffer]
    }

    def unmap(): Unit = {
        mappedByteBuffer.foreach(ByteBufferSupport.unmap)
    }

    override def getFile: File = file
}

object MMapLogWriter {

    def apply(file: File, fileNumber: Long): MMapLogWriter = {
        new MMapLogWriter(file, fileNumber)
    }

    val PAGE_SIZE = 1024 * 1024
}