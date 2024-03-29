package ru.yandex.mysqlDiff
package diff

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

trait TableEntryDropDiff extends TableEntryDiff
trait TableEntryCreateDiff extends TableEntryDiff
trait TableEntryChangeDiff extends TableEntryDiff

abstract class ColumnDiff extends TableEntryDiff
case class CreateColumnDiff(column: ColumnModel) extends ColumnDiff with TableEntryCreateDiff
case class DropColumnDiff(name: String) extends ColumnDiff with TableEntryDropDiff
case class ChangeColumnDiff(override val name: String, override val renameTo: Option[String],
        diff: Seq[ColumnPropertyDiff])
    extends ColumnDiff with TableEntryChangeDiff with ChangeSomethingDiff
{
    def changeDiff = diff.flatMap { case c: ChangeColumnPropertyDiff => Some(c); case _ => None }
    
    def flatten =
        diff.map(x => ChangeColumnDiff(name, renameTo, List(x)))
}

/// special entry used while serializing
case class CreateColumnWithInlinePrimaryKeyCommand(column: ColumnModel, pk: PrimaryKeyModel)
    extends ColumnDiff with TableEntryCreateDiff

/** Diff of TableExtra */
abstract class ExtraDiff extends TableEntryDiff

case class CreateExtraDiff(extra: TableExtra) extends ExtraDiff with TableEntryCreateDiff
case class DropExtraDiff(extra: TableExtra) extends ExtraDiff with TableEntryDropDiff
case class ChangeExtraDiff(oldExtra: TableExtra, newExtra: TableExtra)
    extends ExtraDiff with TableEntryChangeDiff

abstract class TableOptionDiff extends TableEntryDiff
case class CreateTableOptionDiff(option: TableOption) extends TableOptionDiff with TableEntryCreateDiff
case class DropTableOptionDiff(option: TableOption) extends TableOptionDiff with TableEntryDropDiff
case class ChangeTableOptionDiff(oldOption: TableOption, newOption: TableOption)
    extends TableOptionDiff with TableEntryChangeDiff

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
        val entriesDiff: Seq[TableEntryDiff])
    extends TableDiff with ChangeSomethingDiff
{
    // must have something in the diff
    //require(renameTo.isDefined || !entriesDiff.isEmpty)
    
    def columnDiff: Seq[ColumnDiff] = entriesDiff.flatMap {
        case d: ColumnDiff => Some(d)
        case _ => None
    }
    def extraDiff: Seq[ExtraDiff] = entriesDiff.flatMap {
        case d: ExtraDiff => Some(d)
        case _ => None
    }
    def tableOptionDiff: Seq[TableOptionDiff] = entriesDiff.flatMap {
        case d: TableOptionDiff => Some(d)
        case _ => None
    }
    
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
            new ChangeTableDiff(name, renameTo, column5._1 ++ extra5._1 ++ option5._1),
            new ChangeTableDiff(name, renameTo, column5._2 ++ extra5._2 ++ option5._2),
            new ChangeTableDiff(name, renameTo, column5._3 ++ extra5._3 ++ option5._3),
            new ChangeTableDiff(name, renameTo, column5._4 ++ extra5._4 ++ option5._4),
            new ChangeTableDiff(name, renameTo, column5._5 ++ extra5._5 ++ option5._5)
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
