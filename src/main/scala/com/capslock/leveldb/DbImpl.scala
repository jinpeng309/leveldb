package com.capslock.leveldb

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.util.concurrent.locks.ReentrantLock

import com.capslock.leveldb.FileName.{FileToFileInfoImplicit, LongToFileNameImplicit}
import com.capslock.leveldb.Loan.loan
import com.capslock.leveldb.comparator.{BytewiseComparator, InternalKeyComparator}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

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
    var pendingOutputs = ListBuffer[Long]()
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
        val liveFiles = pendingOutputs.toList ::: versions.liveFiles().map(fileMetaData => fileMetaData.fileNumber)
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


    def buildTable(data: SeekingIterable[InternalKey, Slice], fileNumber: Long): Try[FileMetaData] = {
        val file = new File(databaseDir, fileNumber.toTableFileName)
        loan(new FileOutputStream(file).getChannel).to[Try[FileMetaData]](channel => {
            var smallest = Option.empty[InternalKey]
            var largest = Option.empty[InternalKey]
            try{
                val tableBuilder = TableBuilder(options, channel, internalKeyComparator.userComparator)
                val iterator = data.iterator
                while(iterator.hasNext){
                    val (key, value) = iterator.next()
                    if(smallest.isEmpty){
                        smallest = Some(key)
                    }
                    largest = Some(key)
                    tableBuilder.add(key.toSlice, value)
                }
                tableBuilder.finish()
                if(smallest.isEmpty){
                    Failure(new IllegalStateException())
                }else{
                    val fileMeta  =FileMetaData(fileNumber, file.length(),smallest.get, largest.get)
                    pendingOutputs -= fileNumber
                    Success(fileMeta)
                }
            } catch{
                case _:IOException =>
                    file.delete()
                    Failure(new IOException())
                case e:Throwable=>
                    Failure(e)
            }
        })
    }

    @throws(classOf[IOException])
    private def writeLevel0Table(memTable: MemTable, versionEdit: VersionEdit, baseVersion: Option[Version]): Unit = {
        require(mutex.isHeldByCurrentThread)
        val fileNumber = versions.getNextFileNumber()
        pendingOutputs += fileNumber

        mutex.unlock()
        val result = buildTable(memTable, fileNumber)
        mutex.lock()
        pendingOutputs -= fileNumber

        result match {
            case Success(fileMeta) =>
                var level = 0
                if (fileMeta.fileSize > 0) {
                    val smallestUserKey = fileMeta.smallest.userKey
                    val largestUserKey = fileMeta.largest.userKey
                    for (version <- baseVersion) {
                        level = version.pickLevelForMemTableOutput(smallestUserKey, largestUserKey)
                    }
                    versionEdit.addFile(level, fileMeta)
                }
            case Failure(e) =>
                throw e
        }
    }

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
