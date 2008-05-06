package ru.yandex.mysqlDiff.model

import script._

/**
 * Serialize model to create statements.
 */
object ModelSerializer {
    def serializeTable(table: TableModel) = CreateTableStatement(table)
    
    def serializeDatabase(db: DatabaseModel) = db.declarations.map(serializeTable _)
    
    def serializeDatabaseToText(db: DatabaseModel) =
        ScriptSerializer.serialize(serializeDatabase(db), ScriptSerializer.Options.multiline)
}

// vim: set ts=4 sw=4 et:
