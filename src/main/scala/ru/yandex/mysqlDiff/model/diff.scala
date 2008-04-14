package ru.yandex.mysqlDiff.model

abstract class DiffType

case class NameDiff(val oldName: String, val newName: String) 
        extends DiffType

case class TypeDiff(val oldType: DataType, val newType: DataType) 
        extends DiffType 

case class ColumnDiff(val oldColunm: ColumnModel, val newColumn: ColumnModel, val diffList: Seq[DiffType]) 
        extends DiffType

case class TableDiff(val oldTable: TableModel, val newTable: TableModel, val diffList: Seq[DiffType]) 
        extends DiffType