package ru.yandex.mysqlDiff.model



abstract class DiffType[A <: SqlObjectType](val from: A, val to: A) 

case class NameDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)

case class DataTypeDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A) 
 

case class NotNullDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)         

abstract class DiffContainter[A <: SqlObjectType, B <: DiffType[A]]
        (override val from: A, override val to:A, val diffList : Seq[B])
        extends DiffType(from: A, to: A)
        
case class ColumnDiff[A <: SqlObjectType, B <: DiffType[A]] 
        (override val from: A, override val to:A, override val diffList : Seq[B])
        extends DiffContainter(from: A, to: A, diffList: Seq[B])

case class TableDiff[A <: SqlObjectType, B <: DiffType[A]] 
        (override val from: A, override val to:A, override val diffList : Seq[B]) 
        extends DiffContainter(from: A, to: A, diffList: Seq[B])

case class ToIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)  

case class FromIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)  
