package com.capslock.leveldb

import java.io.{File, IOException, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Created by capslock.
 */
class MMapLogWriter(file: File, fileNumber: Long) extends LogWriter {
    val fileChannel = new RandomAccessFile(file, "rw").getChannel
    var mappedByteBuffer = Option(fileChannel.map(MapMode.READ_WRITE, 0, MMapLogWriter.PAGE_SIZE))
    val closed = new AtomicBoolean()
    var fileOffset = 0

    override def isClosed: Boolean = closed.get()

    @throws(classOf[IOException])
    override def addRecord(record: Slice, force: Boolean): Unit = ???

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
    val PAGE_SIZE = 1024 * 1024
}