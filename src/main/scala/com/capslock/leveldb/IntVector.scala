package com.capslock.leveldb

import scala.collection.mutable

/**
 * Created by capslock.
 */
class IntVector() {
    val values = mutable.MutableList.empty[Int]

    def size = values.size

    def clear() = values.clear()

    def add(value: Int) = values += value

    def write(sliceOutput: SliceOutput) = values.foreach(value => sliceOutput.writeInt(value))
}

object IntVector {
    def apply(): IntVector = {
        new IntVector()
    }
}
