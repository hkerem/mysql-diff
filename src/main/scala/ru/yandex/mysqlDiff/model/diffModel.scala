package ru.yandex.mysqlDiff.model

abstract class AbstractDiffModel

case class SourceIsEmpty extends AbstractDiffModel
case class DestinationIsEmpty extends AbstractDiffModel


abstract class AbstractDiffContainer(val sourceName: String,
        val destinationName: String,
        val containerDiff: Seq[AbstractDiffModel],
        val contentDiff: Seq[AbstractDiffModel]
        ) extends AbstractDiffModel

abstract class ColumnDiff

case class CreateColumn(column: ColumnModel) extends ColumnDiff
case class DropColumn(name: String) extends ColumnDiff
case class AlterColumn(name: String, renameTo: Option[String], diff: Seq[ColumnPropertyDiff]) extends ColumnDiff

case class ColumnPropertyDiff(propertyType: PropertyType, oldValue1: Any, newValue1: Any) {
    type T = propertyType.ValueType

    def oldValue: T = oldValue1.asInstanceOf[T]
    def newValue: T = newValue1.asInstanceOf[T]
}



abstract class TableDiff
case class CreateTable(table: TableModel) extends TableDiff
case class DropTable(name: String) extends TableDiff
case class AlterTable(name: String, renameTo: Option[String],
        columnDiff: Seq[ColumnDiff], indexDiff: Seq[IndexDiff]) extends TableDiff
{
    def newName = renameTo getOrElse name
}

abstract class IndexDiff

case class CreateIndex(index: IndexModel) extends IndexDiff
case class DropIndex(name: String) extends IndexDiff
case class AlterIndex(name: String, index: IndexModel) extends IndexDiff

abstract class AbstractPrimaryKeyDiff extends IndexDiff

case class CreatePrimaryKey(pk: PrimaryKey) extends AbstractPrimaryKeyDiff
case object DropPrimaryKey extends AbstractPrimaryKeyDiff
case class AlterPrimaryKey(oldPk: PrimaryKey, newPk: PrimaryKey) extends AbstractPrimaryKeyDiff


case class DatabaseDiff(tableDiff: Seq[TableDiff])

// vim: set ts=4 sw=4 et:
