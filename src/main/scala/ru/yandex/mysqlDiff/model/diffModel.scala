package ru.yandex.mysqlDiff.model

abstract class ColumnDiff

case class CreateColumnDiff(column: ColumnModel) extends ColumnDiff
case class DropColumnDiff(name: String) extends ColumnDiff
case class ChangeColumnDiff(name: String, renameTo: Option[String], diff: Seq[ColumnPropertyDiff]) extends ColumnDiff

case class ColumnPropertyDiff(propertyType: PropertyType, oldValue1: Any, newValue1: Any) {
    type T = propertyType.ValueType

    def oldValue: T = oldValue1.asInstanceOf[T]
    def newValue: T = newValue1.asInstanceOf[T]
}


abstract class IndexDiff

case class CreateIndex(index: IndexModel) extends IndexDiff
case class DropIndex(name: String) extends IndexDiff
case class AlterIndex(name: String, index: IndexModel) extends IndexDiff

abstract class PrimaryKeyDiff extends IndexDiff

case class CreatePrimaryKey(pk: PrimaryKey) extends PrimaryKeyDiff
case object DropPrimaryKey extends PrimaryKeyDiff
case class AlterPrimaryKey(oldPk: PrimaryKey, newPk: PrimaryKey) extends PrimaryKeyDiff


abstract class TableDiff

case class CreateTableDiff(table: TableModel) extends TableDiff
case class DropTableDiff(name: String) extends TableDiff
case class ChangeTableDiff(name: String, renameTo: Option[String],
        columnDiff: Seq[ColumnDiff], indexDiff: Seq[IndexDiff]) extends TableDiff
{
    def newName = renameTo getOrElse name
}


case class DatabaseDiff(tableDiff: Seq[TableDiff])

// vim: set ts=4 sw=4 et:
