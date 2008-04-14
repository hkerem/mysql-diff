package ru.yandex.mysqlDiff.model

abstract class DiffType[A >: SqlObjectType](val from: A, val to: A) 

case class NameDiff 
        extends DiffType

case class TypeDiff 
        extends DiffType 

case class NotNullDiff 
        extends DiffType        

abstract class DiffContainter[A >: SqlObjectType](val from: A, val to:A, val diffList : Seq[DiffType])
        extends DiffType
        
case class ColumnDiff 
        extends DiffContainer

case class TableDiff 
        extends DiffContainer