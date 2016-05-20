package com.capslock.leveldb

/**
 * Created by capslock.
 */
case class LookupResult(key: LookupKey, value: Slice, deleted: Boolean)

object LookupResult {
    def ok(key: LookupKey, value: Slice): LookupResult = {
        LookupResult(key, value, deleted = false)
    }

    def deleted(key: LookupKey): LookupResult = {
        LookupResult(key, Slice.empty, deleted = true)
    }
}