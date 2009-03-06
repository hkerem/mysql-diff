package ru.yandex.mysqlDiff.vendor.mysql

import script._

object MysqlAlterTableStatement {
    import AlterTableStatement._
    
    case class ConvertToCharacterSet(name: String, collate: Option[String]) extends Operation
    case class ChangeCharacterSet(name: String, collate: Option[String]) extends Operation
}

// vim: set ts=4 sw=4 et:
