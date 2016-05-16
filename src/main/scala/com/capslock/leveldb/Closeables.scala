package com.capslock.leveldb

import java.io.IOException

/**
 * Created by capslock.
 */
object Closeables {
    def closeQuietly(closeable: AutoCloseable) {
        if (closeable == null) {
            return
        }
        try {
            closeable.close()
        }
        catch {
            case ignored: IOException => {
            }
        }
    }
}
