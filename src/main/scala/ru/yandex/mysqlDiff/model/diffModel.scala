package ru.yandex.mysqlDiff.model

abstract class ColumnDiff

trait ChangeSomethingDiff {
    def name: String
    def renameTo: Option[String]
    def newName = renameTo getOrElse name
}

case class CreateColumnDiff(column: ColumnModel) extends ColumnDiff
case class DropColumnDiff(name: String) extends ColumnDiff
case class ChangeColumnDiff(override val name: String, override val renameTo: Option[String],
        diff: Seq[ColumnPropertyDiff])
        extends ColumnDiff with ChangeSomethingDiff

case class ColumnPropertyDiff(propertyType: PropertyType, oldValue1: Any, newValue1: Any) {
    type T = propertyType.ValueType

    def oldValue: T = oldValue1.asInstanceOf[T]
    def newValue: T = newValue1.asInstanceOf[T]
}


abstract class IndexDiff

case class CreateIndexDiff(index: IndexModel) extends IndexDiff
case class DropIndexDiff(name: String) extends IndexDiff
case class ChangeIndexDiff(name: String, index: IndexModel) extends IndexDiff

abstract class PrimaryKeyDiff extends IndexDiff

case class CreatePrimaryKeyDiff(pk: PrimaryKey) extends PrimaryKeyDiff
case object DropPrimaryKeyDiff extends PrimaryKeyDiff
case class ChangePrimaryKeyDiff(oldPk: PrimaryKey, newPk: PrimaryKey) extends PrimaryKeyDiff


abstract class TableDiff

case class CreateTableDiff(table: TableModel) extends TableDiff
case class DropTableDiff(name: String) extends TableDiff
case class ChangeTableDiff(override val name: String, override val renameTo: Option[String],
        columnDiff: Seq[ColumnDiff], indexDiff: Seq[IndexDiff])
        extends TableDiff with ChangeSomethingDiff


case class DatabaseDiff(tableDiff: Seq[TableDiff])

// vim: set ts=4 sw=4 et:
