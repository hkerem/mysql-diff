package ru.yandex.mysqlDiff.model



abstract class DiffType[A <: SqlObjectType](val from: A, val to: A) 

case class NameDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)

case class DataTypeDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A) 

case class NotNullDiff[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)         

abstract class DiffContainter[A <: SqlObjectType]
        (override val from: A, override val to:A, val diffList : Seq[DiffType[SqlObjectType]])
        extends DiffType(from: A, to: A)
        
case class ColumnDiff[A <: SqlObjectType] 
        (override val from: A, override val to:A, override val diffList : Seq[DiffType[SqlObjectType]])
        extends DiffContainter(from: A, to: A, diffList: Seq[DiffType[SqlObjectType]])

case class TableDiff[A <: SqlObjectType] 
        (override val from: A, override val to:A, override val diffList : Seq[DiffType[SqlObjectType]]) 
        extends DiffContainter(from: A, to: A, diffList: Seq[DiffType[SqlObjectType]])

case class ToIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)  

case class FromIsNull[A <: SqlObjectType](override val from: A, override val to:A) 
        extends DiffType(from: A, to: A)  
