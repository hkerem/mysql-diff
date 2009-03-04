package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._
import Implicits._

/**
 * Serialize model to create statements (except for methods with toText or toString methods).
 */
object ModelSerializer {
    
    def serializeColumn(column: ColumnModel) =
        new CreateTableStatement.Column(column.name, column.dataType, column.properties)
    
    def serializePk(pk: PrimaryKeyModel) =
        new CreateTableStatement.PrimaryKey(pk)
    
    def serializeRegularIndex(index: IndexModel) =
        new CreateTableStatement.Index(index)
    
    def serializeForeignKey(fk: ForeignKeyModel) =
        new CreateTableStatement.ForeignKey(fk)
    
    def serializeKey(index: KeyModel) = index match {
        case pk: PrimaryKeyModel => serializePk(pk)
        case i: IndexModel => serializeRegularIndex(i)
        case fk: ForeignKeyModel => serializeForeignKey(fk)
    }
    
    def serializeTable(table: TableModel) =
        CreateTableStatement(table.name, false,
            table.columns.map(serializeColumn _) ++ table.allKeys.map(serializeKey _), table.options.properties)

    def serializeTableToText(table: TableModel) =
        ScriptSerializer.serialize(serializeTable(table))
    
    def serializeDatabaseDeclaration(dd: DatabaseDeclaration) = dd match {
        case table: TableModel => serializeTable(table)
    }
    
    def serializeDatabase(db: DatabaseModel) = db.declarations.map(serializeDatabaseDeclaration _)
    
    def serializeDatabaseToText(db: DatabaseModel) =
        ScriptSerializer.serialize(serializeDatabase(db), ScriptSerializer.Options.multiline)
}

// vim: set ts=4 sw=4 et:
