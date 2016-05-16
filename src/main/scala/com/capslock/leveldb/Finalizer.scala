package com.capslock.leveldb

import scala.concurrent.Future

/**
 * Created by capslock.
 */
class Finalizer {
    import scala.concurrent.ExecutionContext.Implicits.global
    def addCloser(closer: () => Unit): Unit = {
        Future{closer}
    }
}
