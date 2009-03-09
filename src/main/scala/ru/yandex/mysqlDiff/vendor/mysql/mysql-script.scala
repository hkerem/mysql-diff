package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._

/** MySQL specific ALTER TABLE operations */
// http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
object MysqlTableDdlStatement {
    import TableDdlStatement._
    
    case class ConvertToCharacterSet(name: String, collate: Option[String]) extends Operation
    case class ChangeCharacterSet(name: String, collate: Option[String]) extends Operation
    
    case class MysqlForeignKey(fk: ForeignKeyModel, indexName: Option[String]) extends Entry
}

// vim: set ts=4 sw=4 et:
