package ru.yandex.mysqlDiff.util

import scala.collection.immutable.Set

trait CollectionImplicits {
    implicit def seqExtras[A](seq: Seq[A]) = new SeqExtras(seq)
    implicit def optionExtras[A](option: Option[A]) = new OptionExtras(option)
}

class SeqExtras[A](seq: Seq[A]) {
    def unique = Set(seq: _*)
}

class OptionExtras[A](option: Option[A]) {
    def getOrThrow(message: => String) = option match {
        case Some(x) => x
        case _ => throw new NoSuchElementException(message)
    }
}

// vim: set ts=4 sw=4 et:
