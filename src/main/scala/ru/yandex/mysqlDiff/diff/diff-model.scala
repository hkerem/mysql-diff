package ru.yandex.mysqlDiff.diff

import model._

import Implicits._

abstract class ColumnPropertyDiff
case class DropColumnPropertyDiff(property: ColumnProperty) extends ColumnPropertyDiff
case class CreateColumnPropertyDiff(property: ColumnProperty) extends ColumnPropertyDiff
case class ChangeColumnPropertyDiff(oldProperty: ColumnProperty, newProperty: ColumnProperty)
    extends ColumnPropertyDiff
{
    require(oldProperty.propertyType == newProperty.propertyType)
    val propertyType = oldProperty.propertyType
}

/** Change some object (table or column) */
trait ChangeSomethingDiff {
    /** Name of object to be changed */
    def name: String
    /** Optional new name, if should be renamed */
    def renameTo: Option[String]
    /** New name of object */
    def newName = renameTo getOrElse name
}

abstract class TableEntryDiff

abstract class ColumnDiff extends TableEntryDiff
case class CreateColumnDiff(column: ColumnModel) extends ColumnDiff
case class DropColumnDiff(name: String) extends ColumnDiff
case class ChangeColumnDiff(override val name: String, override val renameTo: Option[String],
        diff: Seq[ColumnPropertyDiff])
    extends ColumnDiff with ChangeSomethingDiff
{
    def changeDiff = diff.flatMap { case c: ChangeColumnPropertyDiff => Some(c); case _ => None }
}

/** Diff of TableExtra */
abstract class ExtraDiff extends TableEntryDiff

case class CreateExtraDiff(extra: TableExtra) extends ExtraDiff
case class DropExtraDiff(extra: TableExtra) extends ExtraDiff
case class ChangeExtraDiff(oldExtra: TableExtra, newExtra: TableExtra)
    extends ExtraDiff

abstract class TableOptionDiff extends TableEntryDiff
case class CreateTableOptionDiff(option: TableOption) extends TableOptionDiff
case class DropTableOptionDiff(option: TableOption) extends TableOptionDiff
case class ChangeTableOptionDiff(oldOption: TableOption, newOption: TableOption)
    extends TableOptionDiff

abstract class TableDiff

case class CreateTableDiff(table: TableModel) extends TableDiff
case class DropTableDiff(name: String) extends TableDiff
case class ChangeTableDiff(override val name: String, override val renameTo: Option[String],
        columnDiff: Seq[ColumnDiff], extraDiff: Seq[ExtraDiff], tableOptionDiff: Seq[TableOptionDiff])
    extends TableDiff with ChangeSomethingDiff
{
    def entriesDiff: Seq[TableEntryDiff] = columnDiff ++ extraDiff ++ tableOptionDiff
}


/**
 * Model of a difference between two databases.
 */
case class DatabaseDiff(tableDiff: Seq[TableDiff])

// vim: set ts=4 sw=4 et:
