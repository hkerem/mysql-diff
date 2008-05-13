package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._

/**
 * Serialize model to create statements.
 */
object ModelSerializer {
    def serializeColumn(column: ColumnModel) =
        CreateTableStatement.Column(column.name, column.dataType, column.properties)
    
    def serializePk(pk: PrimaryKey) =
        new CreateTableStatement.PrimaryKey(pk)
    
    def serializeRegularIndex(index: IndexModel) =
        new CreateTableStatement.Index(index)
    
    def serializeIndex(index: IndexModel) = index match {
        case pk: PrimaryKey => serializePk(pk)
        case i: IndexModel => serializeRegularIndex(i)
    }
    
    def serializeTable(table: TableModel) =
        new CreateTableStatement(table.name, false,
            table.columns.map(serializeColumn _) ++ table.allIndexes.map(serializeIndex _))
    
    def serializeDatabase(db: DatabaseModel) = db.declarations.map(serializeTable _)
    
    def serializeDatabaseToText(db: DatabaseModel) =
        ScriptSerializer.serialize(serializeDatabase(db), ScriptSerializer.Options.multiline)
}

// vim: set ts=4 sw=4 et:
