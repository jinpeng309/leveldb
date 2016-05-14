package com.capslock.leveldb

/**
 * Created by capslock.
 */
object CompressionType extends Enumeration {
    type CompressionType = Value
    val NONE, SNAPPY = Value
}