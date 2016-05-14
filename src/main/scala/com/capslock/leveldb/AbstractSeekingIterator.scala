package com.capslock.leveldb

/**
 * Created by capslock.
 */
abstract class AbstractSeekingIterator[K, V] extends SeekingIterator[K, V] {
    type Entry = (K, V)
    var nextElement = Option.empty[Entry]

    def seekToFirst() {
        nextElement = Option.empty
        seekToFirstInternal()
    }

    def seek(targetKey: K) {
        nextElement = null
        seekInternal(targetKey)
    }

    def hasNext: Boolean = {
        if (nextElement.isEmpty) {
            nextElement = getNextElement()
        }
        nextElement.isDefined
    }

    def next: Entry = {
        nextElement match {
            case Some(entry) => nextElement = Option.empty; entry
            case _ => if (hasNext) nextElement.get else throw new NoSuchElementException
        }
    }

    def peek: Entry = {
        nextElement match {
            case Some(entry) => entry
            case _ => if (hasNext) nextElement.get else throw new NoSuchElementException
        }
    }

    def remove() {
        throw new UnsupportedOperationException
    }

    protected def seekToFirstInternal()

    protected def seekInternal(targetKey: K)

    protected def getNextElement(): Option[Entry]
}