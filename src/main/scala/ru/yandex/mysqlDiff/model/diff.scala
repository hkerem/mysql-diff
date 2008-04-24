package ru.yandex.mysqlDiff.model

abstract class DiffType[A <: SqlObjectType](val from: A, val to: A) 

case class NameDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)

case class DataTypeDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A) 
 

case class NotNullDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)         

case class AutoIncrementDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)



//keys diff
case class PrimaryKeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)

case class UniqueKeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)

case class IndexKeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)

case class KeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)

case class ConstraintKeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)

case class FulltextKeyDiff[A <: SqlObjectType](override val from: A, override val to:A)
        extends DiffType(from: A, to: A)
//end keys diff


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

case class DatabaseDiff[A <: SqlObjectType, B <: DiffType[A]]
        (override val from: A, override val to:A, override val diffList : Seq[B])
        extends DiffContainter(from: A, to: A, diffList: Seq[B])