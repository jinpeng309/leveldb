package com.capslock.leveldb

/**
 * Created by capslock.
 */
abstract sealed class ValueType

case object DELETION extends ValueType

case object VALUE extends ValueType

object ValueType {
    def fromByte(value: Byte): ValueType = {
        value match {
            case 0 => DELETION
            case 1 => VALUE
        }
    }

    def toByte(valueType: ValueType): Byte = {
        valueType match {
            case DELETION => 0
            case VALUE => 1
        }
    }
}