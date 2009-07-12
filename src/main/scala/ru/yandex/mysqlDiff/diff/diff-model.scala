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

abstract class TableDiff extends DatabaseDeclDiff

case class CreateTableDiff(table: TableModel) extends TableDiff {
    def cutForeignKeys: (CreateTableDiff, Seq[ForeignKeyModel]) =
        (CreateTableDiff(table.dropForeignKeys), table.foreignKeys)
}
case class DropTableDiff(table: TableModel) extends TableDiff {
    def cutForeignKeys: (DropTableDiff, Seq[ForeignKeyModel]) =
        (DropTableDiff(table.dropForeignKeys), table.foreignKeys)
}
case class ChangeTableDiff(override val name: String, override val renameTo: Option[String],
        columnDiff: Seq[ColumnDiff], extraDiff: Seq[ExtraDiff], tableOptionDiff: Seq[TableOptionDiff])
    extends TableDiff with ChangeSomethingDiff
{
    // must have something in the diff
    //require(renameTo.isDefined || !entriesDiff.isEmpty)
    
    def entriesDiff: Seq[TableEntryDiff] = columnDiff ++ extraDiff ++ tableOptionDiff
    
    def splitForOrder: (ChangeTableDiff, ChangeTableDiff, ChangeTableDiff, ChangeTableDiff, ChangeTableDiff) = {
        val column5 = columnDiff.partition5 {
            case _: DropColumnDiff => 2
            case _: ChangeColumnDiff => 3
            case _: CreateColumnDiff => 4
        }
        val extra5 = extraDiff.partition5 {
            case DropExtraDiff(e) =>
                e match {
                    case _: ForeignKeyModel => 1
                    case _ => 2
                }
            case _: ChangeExtraDiff => 3
            case CreateExtraDiff(e) =>
                e match {
                    case _: ForeignKeyModel => 5
                    case _ => 4
                }
        }
        val option5 = tableOptionDiff.partition5 {
            case _: DropTableOptionDiff => 2
            case _: ChangeTableOptionDiff => 3
            case _: CreateTableOptionDiff => 4
        }
        (
            new ChangeTableDiff(name, renameTo, column5._1, extra5._1, option5._1),
            new ChangeTableDiff(name, renameTo, column5._2, extra5._2, option5._2),
            new ChangeTableDiff(name, renameTo, column5._3, extra5._3, option5._3),
            new ChangeTableDiff(name, renameTo, column5._4, extra5._4, option5._4),
            new ChangeTableDiff(name, renameTo, column5._5, extra5._5, option5._5)
        )
    }
}

abstract class SequenceDiff extends DatabaseDeclDiff

case class CreateSequenceDiff(sequence: SequenceModel) extends SequenceDiff
case class DropSequenceDiff(sequence: SequenceModel) extends SequenceDiff
// XXX: alter sequence diff

abstract class DatabaseDeclDiff

/**
 * Model of a difference between two databases.
 */
case class DatabaseDiff(declDiff: Seq[DatabaseDeclDiff]) {
    def ++(that: DatabaseDiff) =
        new DatabaseDiff(this.declDiff ++ that.declDiff)
}

// vim: set ts=4 sw=4 et:
