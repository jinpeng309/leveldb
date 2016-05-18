package com.capslock.leveldb

import com.google.common.base.Charsets

/**
 * Created by capslock.
 */
sealed abstract class VersionEditTag(val persistentId: Int) {
    def readValue(sliceInput: SliceInput, versionEdit: VersionEdit)

    def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit)
}

case object COMPARATOR extends VersionEditTag(1) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        val bytes = Array.fill[Byte](VariableLengthQuantity.readVariableLengthInt(sliceInput))(0)
        sliceInput.readBytes(bytes)
        versionEdit.comparatorName = Option[String](new String(bytes, Charsets.UTF_8))
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        for (comparatorName <- versionEdit.comparatorName) {
            VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
            val bytes = comparatorName.getBytes(Charsets.UTF_8)
            VariableLengthQuantity.writeVariableLengthInt(bytes.length, sliceOutput)
            sliceOutput.writeBytes(bytes)
        }
    }
}

case object LOG_NUMBER extends VersionEditTag(2) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        versionEdit.logNumber = Option(VariableLengthQuantity.readVariableLengthLong(sliceInput))
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        for (logNumber <- versionEdit.logNumber) {
            VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
            VariableLengthQuantity.writeVariableLengthLong(logNumber, sliceOutput)
        }
    }
}

case object PREVIOUS_LOG_NUMBER extends VersionEditTag(9) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        versionEdit.previousLogNumber = Option(VariableLengthQuantity.readVariableLengthLong(sliceInput))
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        for (previousLogNumber <- versionEdit.previousLogNumber) {
            VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
            VariableLengthQuantity.writeVariableLengthLong(previousLogNumber, sliceOutput)
        }
    }
}

case object NEXT_FILE_NUMBER extends VersionEditTag(3) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        versionEdit.nextFileNumber = Option(VariableLengthQuantity.readVariableLengthLong(sliceInput))
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        for (nextFileNumber <- versionEdit.nextFileNumber) {
            VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
            VariableLengthQuantity.writeVariableLengthLong(nextFileNumber, sliceOutput)
        }
    }
}

case object LAST_SEQUENCE extends VersionEditTag(4) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        versionEdit.lastSequenceNumber = Option(VariableLengthQuantity.readVariableLengthLong(sliceInput))
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        for (nextFileNumber <- versionEdit.lastSequenceNumber) {
            VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
            VariableLengthQuantity.writeVariableLengthLong(nextFileNumber, sliceOutput)
        }
    }
}

case object COMPACT_POINTER extends VersionEditTag(5) {
    override def readValue(sliceInput: SliceInput, versionEdit: VersionEdit): Unit = {
        import InternalKey.SliceToInternalKeyImplicit
        val level = VariableLengthQuantity.readVariableLengthInt(sliceInput)
        val keySlice = sliceInput.readBytes(VariableLengthQuantity.readVariableLengthInt(sliceInput))
        versionEdit.setCompactPoint(level, keySlice.toInternalKey)
    }

    override def writeValue(sliceOutput: SliceOutput, versionEdit: VersionEdit): Unit = {
        import InternalKey.InternalKeyToSliceImplicit
        versionEdit.compactPointers.foreach {
            case (level: Int, key: InternalKey) =>
                VariableLengthQuantity.writeVariableLengthInt(persistentId, sliceOutput)
                VariableLengthQuantity.writeVariableLengthInt(level, sliceOutput)
                val keySlice = key.toSlice
                VariableLengthQuantity.writeVariableLengthInt(keySlice.length, sliceOutput)
                sliceOutput.writeBytes(keySlice)

        }
    }
}

object VersionEditTag {
    def getVersionEditTagByPersistentId(persistentId: Int): Option[VersionEditTag] = {
        persistentId match {
            case COMPARATOR.persistentId => Option(COMPARATOR)
            case _ => None
        }
    }

}

