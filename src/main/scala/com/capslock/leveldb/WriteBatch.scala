package com.capslock.leveldb

/**
 * Created by capslock.
 */
class WriteBatch {
    var approximateSize = 0
    private var batch = List[(Slice, Option[Slice])]()

    def size = batch.length

    def put(key: Array[Byte], value: Array[Byte]): WriteBatch = {
        put(Slice(key), Slice(value))
    }

    def put(key: Slice, value: Slice): WriteBatch = {
        batch = batch ::: List((key, Some(value)))
        approximateSize += 12 + key.length + value.length
        this
    }

    def delete(key: Array[Byte]): WriteBatch = {
        delete(Slice(key))
    }

    def delete(key: Slice): WriteBatch = {
        batch = batch ::: List((key, Option.empty))
        approximateSize += 6 + key.length
        this
    }

    def foreach(handler: Handler): Unit = {
        batch foreach {
            case (key, Some(value)) =>
                handler.put(key, value)
            case (key, _) =>
                handler.delete(key)
        }
    }
}

trait Handler {
    def put(key: Slice, value: Slice)

    def delete(key: Slice)
}

object WriteBatch{
    def apply(): WriteBatch ={
        new WriteBatch()
    }
}
