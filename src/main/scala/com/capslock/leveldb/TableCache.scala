package com.capslock.leveldb

import java.io.{File, FileInputStream}

import com.capslock.leveldb.comparator.UserComparator
import org.feijoas.mango.common.cache.CacheBuilder

import scala.util.Success

/**
 * Created by capslock.
 */
class TableCache(databaseDir: File, tableCacheSize: Int, userComparator: UserComparator, verifyChecksum: Boolean) {
    val cache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
        .removalListener[Long, TableAndFile](notification => notification.value.get.table.closer)
        .build((fileNumber: Long) => TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksum))

    private def getTable(fileNumber: Long): Option[Table] = {
        cache.get(fileNumber).map(tableAndFile => tableAndFile.table) match {
            case Success(table) => Some(table)
            case _ => Option.empty
        }

    }

    def newIterator(fileNumber: Long): Option[InternalTableIterator] = {
        getTable(fileNumber).map(table => InternalTableIterator(table.iterator()))
    }

    def newIterator(fileMetaData: FileMetaData): Option[InternalTableIterator] = newIterator(fileMetaData.fileNumber)

    def getApproximateOffsetOf(file: FileMetaData, key: Slice): Long = {
        getTable(file.fileNumber).map(table => table.getApproximateOffsetOf(key)).getOrElse(-1)
    }

    def evict(fileNumber: Long): Unit = cache.invalidate(fileNumber)
}

class TableAndFile(databaseDir: File, fileNumber: Long, userComparator: UserComparator, verifyChecksum: Boolean) {

    import FileName.LongToFileNameImplicit

    private val tableFile = {
        val tableFileName = fileNumber.toTableFileName
        new File(databaseDir, tableFileName)
    }
    val fileChannel = new FileInputStream(tableFile).getChannel
    val table = new MMapTable(tableFile.getAbsolutePath, fileChannel, userComparator, verifyChecksum)
}

object TableAndFile {
    def apply(databaseDir: File, fileNumber: Long, userComparator: UserComparator, verifyChecksum: Boolean) = {
        new TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksum)
    }
}
