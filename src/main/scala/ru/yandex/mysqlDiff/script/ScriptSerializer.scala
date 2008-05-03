package ru.yandex.mysqlDiff.script

import scala.collection.mutable.ArrayBuffer

import model._

import SerializedScript._

object ScriptSerializer {
    def serialize(stmt: ScriptElement): SerializedScript = stmt match {
        case s: ScriptStatement => serializeStatement(s)
        case Unparsed(q) => q
        case CommentElement(c) => c
    }
    
    def serializeStatement(stmt: ScriptStatement): SerializedScript = stmt match {
        case CreateTableStatement(t) => serializeCreateTable(t)
        case DropTableStatement(n) => serializeDropTable(n)
    }
    
    def serializeCreateTable(createTable: CreateTableStatement): SerializedScript =
        serializeCreateTable(createTable.table)
    
    def serializeCreateTable(table: TableModel): SerializedScript = {
        val l =
            table.columns.map(serializeColumn _) ++
            table.primaryKey.map(serializePrimaryKey _) ++
            table.keys.map(serializeIndex _)
            .reverse
        val lines = (List(l.first) ++ l.drop(1).map(_ + ",")).reverse.indent(1)
        
        new SerializedScript(List(ScriptLine("CREATE TABLE " + table.name + "(", 0)) ++ lines.lines ++ List(ScriptLine(")", 0)))
    }
    
    def serializeDropTable(tableName: String) =
        "DROP TABLE " + tableName
    
    def serializeDataType(dataType: DataType) = {
        // XXX: dirty
        var charset = ""
        if (dataType.characterSet != "" && dataType.characterSet != null) charset = " CHARACTER SET " + dataType.characterSet
        var collate = ""
        if (dataType.collate != "" && dataType.collate != null) collate = " COLLATE " + dataType.collate
        var unsigned = ""
        if (dataType.isUnsigned) unsigned = " UNSIGNED"
        var zerofill = ""
        if (dataType.isZerofill) zerofill = " ZEROFILL"
        var size = ""
        if (dataType.length.isDefined) size = "(" + dataType.length.get + ")"
        val result = dataType.name + size + charset + collate + unsigned + zerofill
        result.trim
    }
    
    def serializeColumn(model: ColumnModel) = {
        val attributes = new ArrayBuffer[String]
        
        attributes += (if (model.isNotNull) "NOT NULL" else "NULL")
        if (model.isAutoIncrement) attributes += "AUTOINCREMENT"
        if (model.defaultValue != "" && model.defaultValue != null) attributes += "DEFAULT " + model.defaultValue
        if (model.comment != "" && model.comment != null) attributes += "COMMENT " + model.comment // XXX: lies
        
        model.name + " " + serializeDataType(model.dataType) +
                (if (attributes.isEmpty) "" else " " + attributes.mkString(" "))
    }
    
    def serializeAlterTable(alterTable: AlterTableStatement) =
        "ALTER TABLE " + alterTable.tableName + " " + serializeAlterTableOperation(alterTable.op)
        
    def serializeAlterTableOperation(op: AlterTableStatement.Operation) = {
        import AlterTableStatement._
        op match {
            case AddColumn(column) => "ADD COLUMN " + serializeColumn(column)
            case ChangeColumn(oldName, column) => "CHANGE COLUMN " + oldName + " " + serializeColumn(column)
            case ModifyColumn(column) => "MODIFY COLUMN " + serializeColumn(column)
            case DropColumn(name) => "DROP COLUMN " + name
        }
    }
    
    def serializePrimaryKey(pk: PrimaryKey) =
        "PRIMARY KEY (" + pk.columns.mkString(", ") + ")"
    
    def serializeIndex(index: IndexModel) =
        (if (index.isUnique) "UNIQUE " else "") + "INDEX " + index.name + "(" + index.columns.mkString(", ") + ")"
}

// vim: set ts=4 sw=4 et:
