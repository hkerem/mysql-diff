package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._
import Implicits._

/**
 * Serialize model to create statements (except for methods with toText or toString methods).
 */
class ModelSerializer(context: Context) {
    import context._
    
    def serializeColumn(column: ColumnModel) =
        new TableDdlStatement.Column(column.name, column.dataType, column.properties)
    
    def serializePk(pk: PrimaryKeyModel) =
        new TableDdlStatement.PrimaryKey(pk)
    
    def serializeRegularIndex(index: IndexModel) =
        new TableDdlStatement.Index(index)
    
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
    
    def serializeTable(table: TableModel) =
        CreateTableStatement(table.name, false,
            table.entries.map(serializeTableEntry _), table.options.properties)

    def serializeTableToText(table: TableModel) =
        scriptSerializer.serialize(serializeTable(table))
    
    def serializeDatabaseDeclaration(dd: DatabaseDeclaration) = dd match {
        case table: TableModel => serializeTable(table)
    }
    
    def serializeDatabase(db: DatabaseModel) = db.declarations.map(serializeDatabaseDeclaration _)
    
    def serializeDatabaseToText(db: DatabaseModel) =
        scriptSerializer.serialize(serializeDatabase(db), ScriptSerializer.Options.multiline)
}

// vim: set ts=4 sw=4 et:
