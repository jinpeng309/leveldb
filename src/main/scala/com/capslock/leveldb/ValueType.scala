package com.capslock.leveldb

/**
 * Created by capslock.
 */

object ValueType extends Enumeration {
    type ValueType = Value
    val DELETION, VALUE = Value
    val FOO = Value(22)
}