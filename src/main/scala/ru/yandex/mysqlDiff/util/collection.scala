package ru.yandex.mysqlDiff.util

import scala.collection.immutable.Set

trait CollectionImplicits {
    implicit def seqExtras[A](seq: Seq[A]) = new SeqExtras(seq)
}

class SeqExtras[A](seq: Seq[A]) {
    def unique = Set(seq: _*)
}

// vim: set ts=4 sw=4 et:
