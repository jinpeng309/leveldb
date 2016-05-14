package com.capslock.leveldb

import java.util.Comparator

import com.capslock.leveldb.BlockIterator.BlockEntry


/**
 * Created by capslock.
 */
class BlockBuilder(estimate: Int, blockRestartInterval: Int, comparator: Comparator[Slice]) {
    val block = DynamicSliceOutput(estimate)
    val restartPositions = IntVector(32)
    restartPositions.add(0)

    var finished = false
    var entryCount = 0
    private var lastKey = Option.empty[Slice]
    private var restartBlockEntryCount = 0

    def reset(): Unit = {
        block.reset()
        entryCount = 0
        finished = false
        lastKey = Option.empty[Slice]
        restartBlockEntryCount = 0
        restartPositions.clear()
        restartPositions.add(0)
    }

    def isEmpty = entryCount == 0

    def add(blockEntry: BlockEntry): Unit = {
        add(blockEntry._1, blockEntry._2)
    }

    def add(key: Slice, value: Slice): Unit = {
        var sharedKeyBytes = 0
        if (restartBlockEntryCount < blockRestartInterval) {
            sharedKeyBytes = Slice.calculateCommonBytes(Option(key), lastKey)
        } else {
            restartPositions.add(block.size)
            restartBlockEntryCount = 0
        }
        restartBlockEntryCount += 1
        val nonSharedKeyBytes = key.length - sharedKeyBytes

        VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block)
        VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block)
        VariableLengthQuantity.writeVariableLengthInt(value.length, block)

        block.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes)
        block.writeBytes(value, 0, value.length)

        lastKey = Option(key)
        entryCount += 1
    }

    def currentEstimate = {
        if (finished) {
            block.size
        } else if (block.size == 0) {
            SizeOf.SIZE_OF_INT
        } else {
            block.size + restartPositions.size * SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_INT
        }
    }

    def finish(): Slice = {
        if (!finished) {
            finished = true
            restartPositions.write(block)
            block.writeInt(restartPositions.size)
        }
        block.slice()
    }
}
