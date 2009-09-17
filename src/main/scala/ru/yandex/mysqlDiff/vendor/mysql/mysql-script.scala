package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._

import Implicits._

/** MySQL specific ALTER TABLE operations */
// http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
object MysqlTableDdlStatement {
    import TableDdlStatement._
    
    case class ConvertToCharacterSet(name: String, collate: Option[String]) extends Operation
    case class ChangeCharacterSet(name: String, collate: Option[String]) extends Operation
    
    case class MysqlForeignKey(fk: ForeignKeyModel, indexName: Option[String]) extends Extra
}

class MysqlScriptSerializer(context: Context) extends ScriptSerializer(context) {
    import TableDdlStatement._
    import MysqlTableDdlStatement._
    
    override def quoteName(name: String) = "`" + name + "`"
    
    override def serializeValue(value: SqlValue) = value match {
        case MysqlBitSetValue(0) => "b'0'"
        case MysqlBitSetValue(n) =>
            var r = n
            var s = "b'"
            while (r != 0) {
                s += r & 1
                r >>= 1
            }
            s += "'"
            s
        case _ => super.serializeValue(value)
    }
    
    def serializeNumericDataType(dt: MysqlNumericDataType) = {
        val words = new ArrayBuffer[String]
        words += dt.name + dt.length.map("(" + _ + dt.decimals.map(", " + _).getOrElse("") + ")").getOrElse("")
        if (dt.unsigned == Some(true)) words += "UNSIGNED"
        if (dt.zerofill == Some(true)) words += "ZEROFILL"
        words.mkString(" ")
    }
    
    def serializeCharacterDataType(dt: MysqlCharacterDataType) = {
        val words = new ArrayBuffer[String]
        words += dt.name + dt.length.map("(" + _ + ")").getOrElse("")
        words ++= dt.charset.map("CHARACTER SET " + _)
        words ++= dt.collate.map("COLLATE " + _)
        words.mkString(" ")
    }
    
    override def serializeModelColumnProperty(p: ColumnProperty) = p match {
        case MysqlOnUpdateCurrentTimestamp(true) => Some("ON UPDATE CURRENT_TIMESTAMP")
        case MysqlOnUpdateCurrentTimestamp(false) => None
        case MysqlComment(comment) => Some("COMMENT "+ serializeString(comment))
        case _ => super.serializeModelColumnProperty(p)
    }
    
    def serializeTextDataType(dt: MysqlTextDataType) = {
        val words = new ArrayBuffer[String]
        words += dt.name
        words ++= dt.binary.map(x => "BINARY")
        words ++= dt.charset.map("CHARACTER SET " + _)
        words ++= dt.collate.map("COLLATE " + _)
        words.mkString(" ")
    }
    
    override def serializeDataType(dataType: DataType) = dataType match {
        // XXX: test
        case MysqlEnumDataType(values) => "ENUM(" + values.map(serializeString _).mkString(", ") + ")"
        case MysqlSetDataType(values) => "SET(" + values.map(serializeString _).mkString(", ") + ")"
        case dt: MysqlCharacterDataType => serializeCharacterDataType(dt)
        case dt: MysqlTextDataType => serializeTextDataType(dt)
        case dt: MysqlNumericDataType => serializeNumericDataType(dt)
        case _ => super.serializeDataType(dataType)
    }
    
    override def serializeTableElement(e: TableDdlStatement.TableElement) = e match {
        case MysqlForeignKey(fk, indexName) =>
            val ForeignKeyModel(name, localColumns, externalTable, externalColumns, updateRule, deleteRule) = fk
            val words = new ArrayBuffer[String]
            words ++= name.map("CONSTRAINT " + _)
            words += "FOREIGN KEY"
            words ++= indexName
            words += "(" + localColumns.map(serializeIndexColumn _).mkString(", ") + ")"
            words += "REFERENCES"
            words += externalTable
            words += ("(" + externalColumns.mkString(", ") + ")")
            words ++= updateRule.map(p => "ON UPDATE " + serializeImportedKeyRule(p))
            words ++= deleteRule.map(p => "ON DELETE " + serializeImportedKeyRule(p))
            words.mkString(" ")
        case _ => super.serializeTableElement(e)
    }
    
}

case class MysqlSetStatement(target: SqlExpr, value: SqlExpr) extends ScriptStatement {
}

// vim: set ts=4 sw=4 et:
