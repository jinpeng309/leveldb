package com.capslock.leveldb

import java.io.{File, FileInputStream, IOException}
import java.util.concurrent.locks.ReentrantLock

import com.capslock.leveldb.FileName.{FileToFileInfoImplicit, LongToFileNameImplicit}
import com.capslock.leveldb.Loan.loan
import com.capslock.leveldb.comparator.{BytewiseComparator, InternalKeyComparator}

/**
 * Created by capslock.
 */
class DbImpl(options: Options, databaseDir: File) {
    databaseDir.mkdir()
    val internalKeyComparator = InternalKeyComparator(BytewiseComparator())
    var memTable = Some(new MemTable(internalKeyComparator))
    var immutableMemTable = Option.empty[MemTable]
    val tableCache = TableCache(databaseDir, options.maxOpenFiles - 10, BytewiseComparator(), options.verifyChecksum)
    val mutex = new ReentrantLock()
    val condition = mutex.newCondition()
    val dbLock = DbLock(new File(databaseDir, FileName.lockFile))
    val versions = VersionSet(databaseDir, tableCache, internalKeyComparator)
    var log: LogWriter = null
    var pendingOutputs = List[Long]()
    recoverDb()

    def recoverDb(): Unit = {
        mutex.lock()
        try {
            versions.recover()
            val minLogNumber = versions.logNumber
            val preLogNumber = versions.preLogNumber
            val fileNames = databaseDir.listFiles()
            val edit = VersionEdit()
            fileNames
                .map(file => file.toFileInfo)
                .filter {
                    case FileInfo(FileType.LOG, fileNumber) if fileNumber >= minLogNumber || fileNumber == preLogNumber => true
                    case _ => false
                }
                .map(fileInfo => fileInfo.fileNumber)
                .sortWith(_ < _)
                .foreach(fileNumber => {
                    val maxSequenceNumber = recoverLogFile(fileNumber, edit)
                    if (maxSequenceNumber > versions.lastSequence) {
                        versions.lastSequence = maxSequenceNumber
                    }
                })
            val logFileNumber = versions.getNextFileNumber()
            log = MMapLogWriter(new File(databaseDir, logFileNumber.toLogFileName), logFileNumber)
            edit.logNumber = Some(log.getFileNumber)
            versions.logAndApply(edit)
            // cleanup unused files
            deleteObsoleteFiles()
            // schedule compactions
            maybeScheduleCompaction()
        } finally {
            mutex.unlock()
        }


    }

    def maybeScheduleCompaction(): Unit = {
        //todo
    }

    def deleteObsoleteFiles() = {
        require(mutex.isHeldByCurrentThread)
        val liveFiles = pendingOutputs ::: versions.liveFiles().map(fileMetaData => fileMetaData.fileNumber)
        databaseDir.listFiles()
            .filterNot(file => {
                file.toFileInfo match {
                    case FileInfo(FileType.LOG, fileNumber) =>
                        fileNumber >= versions.logNumber || fileNumber == versions.preLogNumber
                    case FileInfo(FileType.DESCRIPTOR, fileNumber) =>
                        fileNumber >= versions.manifestFileNumber
                    case FileInfo(fileType, fileNumber) if fileType == FileType.TABLE || fileType == FileType.TEMP =>
                        liveFiles.contains(fileNumber)
                    case _ => true
                }
            })
            .foreach(file => {
                file.toFileInfo match {
                    case FileInfo(FileType.TABLE, fileNumber) =>
                        tableCache.evict(fileNumber)
                }
                file.delete()
            })
    }


    def writeLevel0Table(memTable: MemTable, versionEdit: VersionEdit, maybeVersion: Option[Version]) = ???

    @throws(classOf[IOException])
    def recoverLogFile(fileNumber: Long, versionEdit: VersionEdit): Long = {
        require(mutex.isHeldByCurrentThread)
        val file = new File(databaseDir, fileNumber.toLogFileName)
        var maxSequenceNumber = 0L
        loan(new FileInputStream(file).getChannel).to[Unit](channel => {
            val logReader = LogReader(channel, verifyChecksum = true, 0)

            Stream
            .continually(logReader.readRecord())
            .takeWhile(record => record.isDefined)
            .foreach(logRecord => {
                for(record <- logRecord){
                    val sliceInput = SliceInput(record)
                    val sequenceBegin = sliceInput.readLong
                    val updateSize = sliceInput.readInt
                    val lastSequenceNumber = sequenceBegin + updateSize - 1

                    if(memTable.isEmpty){
                        memTable = Some(new MemTable(internalKeyComparator))
                    }
                    val memoryTable = memTable.get

                    val writeBatch = readWriteBatch(sliceInput, updateSize)
                    writeBatch.foreach(new Handler {
                        var sequenceNumber =sequenceBegin
                        override def put(key: Slice, value: Slice): Unit = {
                            memoryTable.add(sequenceNumber, ValueType.VALUE, key, value)
                            sequenceNumber += 1
                        }
                        override def delete(key: Slice): Unit =
                            memoryTable.add(sequenceNumber, ValueType.DELETION, key, Slice.empty)
                            sequenceNumber += 1
                        })

                    if(lastSequenceNumber > maxSequenceNumber){
                        maxSequenceNumber = lastSequenceNumber
                    }
                    for(memoryTable <- memTable; if memoryTable.approximateMemoryUsage > options.writeBufferSize){
                        writeLevel0Table(memoryTable, versionEdit, Option.empty)
                    }
                }
            })

            for(memoryTable <- memTable){
                writeLevel0Table(memoryTable, versionEdit, Option.empty)
            }
            return maxSequenceNumber
        })
        maxSequenceNumber
    }

    @throws(classOf[IOException])
    def readWriteBatch(entryInput: SliceInput, updateSize: Int): WriteBatch = {
        val writeBatch = WriteBatch()

        def readEntry(): Boolean = {
            if (entryInput.isReadable) {
                val valueType = ValueType(entryInput.readByte())
                valueType match {
                    case ValueType.VALUE =>
                        writeBatch.put(entryInput.readLengthPrefixedBytes(), entryInput.readLengthPrefixedBytes())
                    case ValueType.DELETION =>
                        writeBatch.delete(entryInput.readLengthPrefixedBytes())
                }
                true
            } else {
                false
            }
        }

        val entrySum = Stream
            .continually(readEntry())
            .takeWhile(readAvailable => readAvailable)
            .foldLeft(0)((sum, _) => sum + 1)

        if (entrySum != updateSize) {
            throw new IOException(s"Expected $updateSize entries in log record but found $entrySum entries")
        }

        writeBatch
    }

}
