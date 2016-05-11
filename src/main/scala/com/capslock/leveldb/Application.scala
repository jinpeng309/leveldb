package com.capslock.leveldb

/**
 * Created by capslock.
 */
object Application extends App {
    val data = Array(1.toByte, 0.toByte)
    println(data.mkString(" "))
    val slice = Slice(data)
    println(slice.getShort(0))
}
