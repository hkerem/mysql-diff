package ru.yandex.mysqlDiff.script

import scala.collection.mutable.ArrayBuffer

import model._

object ScriptSerializer {
    def serializeStatement(stmt: ScriptStatement): String = throw new AssertionError
    
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
    
    def serializePrimaryKey(pk: PrimaryKey) =
        "PRIMARY KEY (" + pk.columns.mkString(", ") + ")"
}

// vim: set ts=4 sw=4 et:
