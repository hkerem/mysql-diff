package ru.yandex.mysqlDiff.model

abstract class AbstractDiffModel

case class SourceIsEmpty extends AbstractDiffModel
case class DestinationIsEmpty extends AbstractDiffModel


abstract class AbstractDiffContainer(val sourceName: String,
        val destinationName: String,
        val containerDiff: Seq[AbstractDiffModel],
        val contentDiff: Seq[AbstractDiffModel]
        ) extends AbstractDiffModel

abstract class AbstractAlterColumn

case class CreateColumn(column: ColumnModel) extends AbstractAlterColumn
case class DropColumn(name: String) extends AbstractAlterColumn
case class AlterColumn(name: String, renameTo: Option[String], diff: Seq[ColumnPropertyDiff]) extends AbstractAlterColumn

case class ColumnPropertyDiff(propertyType: PropertyType, oldValue1: Any, newValue1: Any) {
    type T = propertyType.ValueType

    def oldValue: T = oldValue1.asInstanceOf[T]
    def newValue: T = newValue1.asInstanceOf[T]
}



abstract class AbstractTableDiff
case class CreateTable(table: TableModel) extends AbstractTableDiff
case class DropTable(name: String) extends AbstractTableDiff
case class TableDiffModel(name: String, renameTo: Option[String], columnDiff: Seq[AbstractAlterColumn], indexDiff: Seq[AbstractIndexDiff]) extends AbstractTableDiff

abstract class AbstractIndexDiff

case class CreateIndex(index: IndexModel) extends AbstractIndexDiff
case class DropIndex(name: String) extends AbstractIndexDiff
case class AlterIndex(name: String, index: IndexModel) extends AbstractIndexDiff

abstract class AbstractPrimaryKeyDiff extends AbstractIndexDiff

case class CreatePrimaryKey(pk: PrimaryKey) extends AbstractPrimaryKeyDiff
case class DropPrimaryKey extends AbstractPrimaryKeyDiff
case class AlterPrimaryKey(oldPk: PrimaryKey, newPk: PrimaryKey) extends AbstractPrimaryKeyDiff


case class DatabaseDiff(tableDiff: Seq[AbstractTableDiff])
