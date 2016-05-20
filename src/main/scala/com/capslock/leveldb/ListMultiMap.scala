package com.capslock.leveldb

/**
 * Created by capslock.
 */
object ListMultiMap {
    implicit class ListMultiMapImplicit[A,B](val map: Map[A, List[B]]) extends AnyVal {

        def addBinding(key: A, value: B): Map[A, List[B]] =
            map + (key -> { value :: map.getOrElse(key, Nil) })

        def removeBinding(key: A, value: B): ListMultiMapImplicit[A, B] = map.get(key) match {
            case None => map
            case Some(List(`value`)) => map - key
            case Some(list) => map + (key -> list.diff(List(value)))
        }
    }
}
