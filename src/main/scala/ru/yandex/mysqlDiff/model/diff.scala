package ru.yandex.mysqlDiff.model

abstract class NameDiff {
}

case object NoNameDiff extends NameDiff

case class DoNameDiff(val oldName: String, val newName: String) extends NameDiff


case class ColumnDiff(val oldColunm: Column, val newColumn: Column) {
    
}

abstract class DatabaseDeclarationDiff

case class TableDiff(nameDiff: NameDiff, val columnDiffs: Seq[ColumnDiff]) extends DatabaseDeclarationDiff {
    
}

case class DatabaseDiff(val declarationDiffs: Seq[DatabaseDeclarationDiff])