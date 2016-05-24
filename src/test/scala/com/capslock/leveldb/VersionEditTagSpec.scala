package com.capslock.leveldb

import com.capslock.leveldb.comparator.{BytewiseComparator, InternalKeyComparator}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.TreeMap

/**
 * Created by capslock.
 */
class VersionEditTagSpec extends FlatSpec with Matchers with MockFactory {
    "readValue writeValue method" should "match each other" in {
        testReadWriteForComparator()
        testLogNumber()
        testPreLogNumber()
        testNextFileNumber()
        testLastSequence()
        testCompactPointer()
        testDeleteFile()
    }

    def testReadWriteForComparator(): Unit = {
        val versionEditTag = VersionEditTag.COMPARATOR
        val versionEditForWrite = VersionEdit()
        val comparatorName = Some("comparator")
        versionEditForWrite.comparatorName = comparatorName

        testEditTagWriteRead(versionEditTag, versionEditForWrite, 1, versionEdit => {
            versionEdit.comparatorName shouldEqual comparatorName
        })
    }

    def testLogNumber(): Unit = {
        val versionEditTag = VersionEditTag.LOG_NUMBER
        val versionEditForWrite = VersionEdit()
        val logNumber = Some(1000L)
        versionEditForWrite.logNumber = logNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, 1, versionEdit => {
            versionEdit.logNumber shouldEqual logNumber
        })
    }

    def testPreLogNumber(): Unit = {
        val versionEditTag = VersionEditTag.PREVIOUS_LOG_NUMBER
        val versionEditForWrite = VersionEdit()
        val preLogNumber = Some(10000L)
        versionEditForWrite.previousLogNumber = preLogNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, 1, versionEdit => {
            versionEdit.previousLogNumber shouldEqual preLogNumber
        })
    }

    def testNextFileNumber(): Unit = {
        val versionEditTag = VersionEditTag.NEXT_FILE_NUMBER
        val versionEditForWrite = VersionEdit()
        val nextFileNumber = Some(100000L)
        versionEditForWrite.nextFileNumber = nextFileNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, 1, versionEdit => {
            versionEdit.nextFileNumber shouldEqual nextFileNumber
        })
    }

    def testLastSequence(): Unit = {
        val versionEditTag = VersionEditTag.LAST_SEQUENCE
        val versionEditForWrite = VersionEdit()
        val lastSequence = Some(100000L)
        versionEditForWrite.lastSequenceNumber = lastSequence

        testEditTagWriteRead(versionEditTag, versionEditForWrite, 1, versionEdit => {
            versionEdit.lastSequenceNumber shouldEqual lastSequence
        })
    }

    def testCompactPointer(): Unit = {
        val versionEditTag = VersionEditTag.COMPACT_POINTER
        val versionEditForWrite = VersionEdit()
        var compactPointers = TreeMap[Int, InternalKey]()
        compactPointers += (0 -> InternalKey(Slice("key0"), 100000L, ValueType.VALUE))
        compactPointers += (1 -> InternalKey(Slice("key1"), 100001L, ValueType.VALUE))
        compactPointers += (2 -> InternalKey(Slice("key2"), 100002L, ValueType.VALUE))
        compactPointers += (3 -> InternalKey(Slice("key3"), 100003L, ValueType.VALUE))
        compactPointers += (4 -> InternalKey(Slice("key4"), 100004L, ValueType.VALUE))
        compactPointers += (5 -> InternalKey(Slice("key5"), 100005L, ValueType.VALUE))
        compactPointers += (6 -> InternalKey(Slice("key6"), 100006L, ValueType.VALUE))

        versionEditForWrite.compactPointers = compactPointers

        testEditTagWriteRead(versionEditTag, versionEditForWrite, compactPointers.size, versionEdit => {
            versionEdit.compactPointers.foreach {
                case (level: Int, key: InternalKey) =>
                    InternalKeyComparator(BytewiseComparator()).compare(compactPointers(level), key) shouldEqual 0
            }
        })
    }

    def testDeleteFile(): Unit = {
        val versionEditTag = VersionEditTag.DELETED_FILE
        val versionEditForWrite = VersionEdit()
        val deleteFiles = Map(1 -> List(1L, 2L, 3L), 2 -> List(11L, 22L, 33L))
        val sumSize = deleteFiles.foldLeft(0) { case (sum, (_, files)) => sum + files.size }
        versionEditForWrite.deleteFiles = deleteFiles

        testEditTagWriteRead(versionEditTag, versionEditForWrite, sumSize, versionEdit => {
            versionEdit.deleteFiles.size shouldEqual deleteFiles.size
            versionEdit.deleteFiles.foreach {
                case (level, fileList) =>
                    fileList.toSet shouldEqual deleteFiles(level).toSet
            }
        })
    }

    def testEditTagWriteRead(versionEditTag: VersionEditTag, versionEditForWrite: VersionEdit, readTimes: Int,
                             checkFunction: (VersionEdit) => Unit): Unit = {
        val resultVersionEdit = writeEditToSliceAndReadEditFromSlice(versionEditTag, versionEditForWrite, readTimes)
        checkFunction(resultVersionEdit)
    }

    def writeEditToSliceAndReadEditFromSlice(versionEditTag: VersionEditTag, versionEdit: VersionEdit, readTimes: Int): VersionEdit = {
        val output = DynamicSliceOutput(10)
        versionEditTag.writeValue(output, versionEdit)
        val input = SliceInput(output.slice())

        val resultVersionEdit = VersionEdit()
        var persistentId = 0
        1 to readTimes foreach { time =>
            persistentId = VariableLengthQuantity.readVariableLengthInt(input)
            persistentId shouldEqual versionEditTag.persistentId
            versionEditTag.readValue(input, resultVersionEdit)
        }
        resultVersionEdit
    }


}
