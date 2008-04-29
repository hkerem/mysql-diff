package ru.yandex.mysqlDiff.model

abstract class DiffType(val from: SqlObjectType, val to: SqlObjectType)

case class NameDiff[A <: SqlObjectType](override val from: A, override val to:A) 
    extends DiffType(from, to)

case class DataTypeDiff(override val from: ColumnModel, override val to: ColumnModel) 
    extends DiffType(from, to)
 

case class NotNullDiff(override val from: ColumnModel, override val to: ColumnModel)
    extends DiffType(from, to)

case class AutoIncrementDiff(override val from: ColumnModel, override val to: ColumnModel)
    extends DiffType(from, to)

case class DefaultValueDiff(override val from: ColumnModel, override val to:ColumnModel) 
    extends DiffType(from, to)

//keys diff
case class PrimaryKeyDiff(override val from: PrimaryKeyModel, override val to: PrimaryKeyModel)
    extends DiffType(from, to)

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


abstract class DiffContainter[A <: SqlObjectType, B <: DiffType]
    (override val from: A, override val to:A, val diffList : Seq[B])
    extends DiffType(from: A, to: A)
        
case class ColumnDiff
    (override val from: ColumnModel, override val to: ColumnModel, override val diffList : Seq[DiffType])
    extends DiffContainter(from, to, diffList)

case class TableDiff
    (override val from: TableModel, override val to:TableModel, override val diffList : Seq[DiffType])
    extends DiffContainter(from, to, diffList)

case class ToIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
    extends DiffType(from: A, to: A)  

case class FromIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
    extends DiffType(from: A, to: A)  

case class DatabaseDiff[A <: SqlObjectType]
    (override val from: A, override val to:A, override val diffList: Seq[DiffType])
    extends DiffContainter(from: A, to: A, diffList)