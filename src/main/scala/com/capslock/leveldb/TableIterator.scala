package com.capslock.leveldb

/**
 * Created by capslock.
 */
class TableIterator(table: Table, blockIterator: BlockIterator) extends AbstractSeekingIterator[Slice, Slice] {
    var current = Option.empty[BlockIterator]

    override protected def seekToFirstInternal(): Unit = {
        seekToFirst()
        current = Option.empty
    }

    override protected def seekInternal(targetKey: Slice): Unit = {
        blockIterator.seek(targetKey)
        if (blockIterator.hasNext) {
            current = getNextBlock()
            current.foreach(blockIterator => blockIterator.seek(targetKey))
        } else {
            current = Option.empty
        }
    }

    override protected def getNextElement(): Option[Entry] = {
        var currentHasNext = false
        while (true) {
            currentHasNext = if (current.isDefined) current.get.hasNext else false
            if (!currentHasNext) {
                current = getNextBlock()
            } else {
                return current.map(blockIterator => blockIterator.next())
            }
        }
        Option.empty
    }

    def getNextBlock(): Option[BlockIterator] = {
        if (blockIterator.hasNext) {
            val blockEntry = blockIterator.next()._2
            table.openBlock(blockEntry).map(block => block.iterator())
        } else {
            Option.empty
        }

    }
}

object TableIterator{
    def apply(table: Table, blockIterator: BlockIterator):TableIterator = {
        new TableIterator(table,blockIterator)
    }
}
