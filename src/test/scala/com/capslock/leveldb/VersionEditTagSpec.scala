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
    }

    def testReadWriteForComparator(): Unit = {
        val versionEditTag = VersionEditTag.COMPARATOR
        val versionEditForWrite = VersionEdit()
        val comparatorName = Some("comparator")
        versionEditForWrite.comparatorName = comparatorName

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.comparatorName shouldEqual comparatorName
        })
    }

    def testLogNumber(): Unit = {
        val versionEditTag = VersionEditTag.LOG_NUMBER
        val versionEditForWrite = VersionEdit()
        val logNumber = Some(1000L)
        versionEditForWrite.logNumber = logNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.logNumber shouldEqual logNumber
        })
    }

    def testPreLogNumber(): Unit = {
        val versionEditTag = VersionEditTag.PREVIOUS_LOG_NUMBER
        val versionEditForWrite = VersionEdit()
        val preLogNumber = Some(10000L)
        versionEditForWrite.previousLogNumber = preLogNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.previousLogNumber shouldEqual preLogNumber
        })
    }

    def testNextFileNumber(): Unit = {
        val versionEditTag = VersionEditTag.NEXT_FILE_NUMBER
        val versionEditForWrite = VersionEdit()
        val nextFileNumber = Some(100000L)
        versionEditForWrite.nextFileNumber = nextFileNumber

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.nextFileNumber shouldEqual nextFileNumber
        })
    }

    def testLastSequence(): Unit = {
        val versionEditTag = VersionEditTag.LAST_SEQUENCE
        val versionEditForWrite = VersionEdit()
        val lastSequence = Some(100000L)
        versionEditForWrite.lastSequenceNumber = lastSequence

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.lastSequenceNumber shouldEqual lastSequence
        })
    }

    def testCompactPointer(): Unit = {
        val versionEditTag = VersionEditTag.COMPACT_POINTER
        val versionEditForWrite = VersionEdit()
        var compactPointers = TreeMap[Int, InternalKey]()
        compactPointers += (1 -> InternalKey(Slice("key"), 10000L, ValueType.VALUE))
        versionEditForWrite.compactPointers = compactPointers

        testEditTagWriteRead(versionEditTag, versionEditForWrite, versionEdit => {
            versionEdit.compactPointers.foreach {
                case (level: Int, key: InternalKey) =>
                    InternalKeyComparator(BytewiseComparator()).compare(compactPointers(level), key) shouldEqual 0
            }
        })
    }

    def testEditTagWriteRead(versionEditTag: VersionEditTag, versionEditForWrite: VersionEdit,
                             checkFunc: (VersionEdit) => Unit): Unit = {
        val (persistentId, resultVersionEdit) = writeEditToSliceAndReadEditFromSlice(versionEditTag, versionEditForWrite)
        persistentId shouldEqual versionEditTag.persistentId
        checkFunc(resultVersionEdit)
    }

    def writeEditToSliceAndReadEditFromSlice(versionEditTag: VersionEditTag, versionEdit: VersionEdit): (Int, VersionEdit) = {
        val output = DynamicSliceOutput(10)
        versionEditTag.writeValue(output, versionEdit)
        val input = SliceInput(output.slice())

        val resultVersionEdit = VersionEdit()
        val persistentId = VariableLengthQuantity.readVariableLengthInt(input)
        versionEditTag.readValue(input, resultVersionEdit)
        (persistentId, resultVersionEdit)
    }


}
