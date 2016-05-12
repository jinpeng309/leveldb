package com.capslock.leveldb

import scala.collection.mutable

/**
 * Created by capslock.
 */
class IntVector(capability: Int) {
    val values = mutable.MutableList(capability)

    def size = values.size

    def clear = values.clear()

    def add(value: Int) = values += value

    def write(sliceOutput: SliceOutput) = values.foreach(value => sliceOutput.writeInt(value))

}
