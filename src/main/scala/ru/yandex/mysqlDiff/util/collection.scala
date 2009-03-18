package ru.yandex.mysqlDiff.util

import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer

trait CollectionImplicits {
    implicit def seqExtras[A](seq: Seq[A]) = new SeqExtras(seq)
    implicit def optionExtras[A](option: Option[A]) = new OptionExtras(option)
}

class SeqExtras[A](seq: Seq[A]) {
    def unique = Set(seq: _*)
    
    def partition3(f: A => Int): (Seq[A], Seq[A], Seq[A]) = {
        val r = (new ArrayBuffer[A], new ArrayBuffer[A], new ArrayBuffer[A])
        for (e <- seq) {
            val b = f(e) match {
                case 1 => r._1
                case 2 => r._2
                case 3 => r._3
            }
            b += e
        }
        r
    }
}

class OptionExtras[A](option: Option[A]) {
    def getOrThrow(message: => String) = option match {
        case Some(x) => x
        case _ => throw new NoSuchElementException(message)
    }
}

object CollectionUtils {
    
    /**
     * Find common elements in two seqs.
     * @return (elementsOnlyInA, elementsOnlyInB, pairsOfElementsInBothSeqs)
     */
    def compareSeqs[A, B](a: Seq[A], b: Seq[B], comparator: (A, B) => Boolean): (Seq[A], Seq[B], Seq[(A, B)]) = {
        var onlyInA = List[A]()
        var onlyInB = List[B]()
        var inBothA = List[(A, B)]()
        var inBothB = List[(A, B)]()
        
        for (x <- a) {
            b.find(comparator(x, _)) match {
                case Some(y) => inBothA ++= List((x, y))
                case None => onlyInA ++= List(x)
            }
        }
        
        for (y <- b) {
            a.find(comparator(_, y)) match {
                case Some(x) => inBothB ++= List((x, y))
                case None => onlyInB ++= List(y)
            }
        }
        
        // inBothA must_== inBothB
        (onlyInA, onlyInB, inBothA)
    }
    
}

object CollectionTests extends org.specs.Specification {
    
    "compareSeqs" in {
        val a = List(1, 2, 3, 5)
        val b = List("4", "3", "2")

        def comparator(x: Int, y: String) = x.toString == y
        val (onlyInA, onlyInB, inBoth) = CollectionUtils.compareSeqs(a, b, comparator _)

        List(1, 5) must_== onlyInA.toList
        List("4") must_== onlyInB.toList
        List((2, "2"), (3, "3")) must_== inBoth.toList
    }
    
}

// vim: set ts=4 sw=4 et:
