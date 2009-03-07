package ru.yandex.mysqlDiff.vendor.mysql

import script._

/** MySQL specific ALTER TABLE operations */
// http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
object MysqlAlterTableStatement {
    import AlterTableStatement._
    
    case class ConvertToCharacterSet(name: String, collate: Option[String]) extends Operation
    case class ChangeCharacterSet(name: String, collate: Option[String]) extends Operation
}

// vim: set ts=4 sw=4 et:
