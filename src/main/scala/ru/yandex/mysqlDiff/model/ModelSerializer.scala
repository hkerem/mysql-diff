package ru.yandex.mysqlDiff
package model

import scala.collection.mutable.ArrayBuffer

import script._
import Implicits._

/**
 * Serialize model to create statements (except for methods with toText or toString methods).
 */
class ModelSerializer(context: Context) {
    import context._
    
    import TableDdlStatement._
    
    def serializeColumn(column: ColumnModel) =
        new TableDdlStatement.Column(column.name, column.dataType, column.properties)
    
    def serializePk(pk: PrimaryKeyModel) =
        new TableDdlStatement.PrimaryKey(pk)
    
    def serializeRegularIndex(index: IndexModel) = {
        require(index.explicit)
        new TableDdlStatement.Index(index)
    }
    
    def serializeForeignKey(fk: ForeignKeyModel) =
        new TableDdlStatement.ForeignKey(fk)
    
    def serializeUniqueKey(uk: UniqueKeyModel) =
        new TableDdlStatement.UniqueKey(uk)
    
    def serializeTableEntry(e: TableEntry) = e match {
        case c: ColumnModel => serializeColumn(c)
        case i: IndexModel => serializeRegularIndex(i)
        case uk: UniqueKeyModel => serializeUniqueKey(uk)
        case pk: PrimaryKeyModel => serializePk(pk)
        case fk: ForeignKeyModel => serializeForeignKey(fk)
    }
    
    protected def splitTable(table: TableModel): (TableModel, Seq[TableExtra]) = {
        val TableModel(name, columns, extras, options) = table
        val (Seq(extrasInside @ _*), Seq(extrasOutside @ _*)) = extras.partition {
            case _: IndexModel => false
            case _ => true
        }
        (TableModel(name, columns, extrasInside, options), extrasOutside)
    }
    
    protected def serializeTableOnly(table: TableModel) =
        CreateTableStatement(table.name, false,
            TableDdlStatement.TableElementList(table.explicitEntries.map(serializeTableEntry _)),
            table.options.properties)
    
    protected def serializeOuterExtra(extra: TableExtra, table: TableModel) = extra match {
        case i: IndexModel =>
            require(i.explicit)
            CreateIndexStatement(
                i.name.getOrThrow("cannot create unnamed index"), table.name, i.columns)
    }
    
    protected def serializeOuterExtras(extras: Seq[TableExtra], table: TableModel) =
        extras
            .filter { case i: IndexModel => i.explicit; case _ => true }
            .map(e => serializeOuterExtra(e, table))
    
    def serializeTable(table: TableModel): Seq[DdlStatement] = {
        val (tableOnly, extras) = splitTable(table)
        List(serializeTableOnly(tableOnly)) ++ serializeOuterExtras(extras, table)
    }
    
    def serializeSequence(sequence: SequenceModel) =
        CreateSequenceStatement(sequence.name)

    def serializeTableToText(table: TableModel) =
        scriptSerializer.serialize(serializeTable(table), ScriptSerializer.Options.multiline)
    
    def serializeDatabaseDecl(dd: DatabaseDecl): Seq[DdlStatement] = dd match {
        case table: TableModel => serializeTable(table)
        case sequence: SequenceModel => List(serializeSequence(sequence))
    }
    
    def serializeDatabase(db: DatabaseModel) = db.decls.flatMap(serializeDatabaseDecl _)
    
    def serializeDatabaseToText(db: DatabaseModel) =
        scriptSerializer.serialize(serializeDatabase(db), ScriptSerializer.Options.multiline)
}

// vim: set ts=4 sw=4 et:
