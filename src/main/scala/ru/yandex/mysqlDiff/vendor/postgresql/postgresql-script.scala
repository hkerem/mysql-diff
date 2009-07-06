package ru.yandex.mysqlDiff.vendor.postgresql

import model._
import script._

import Implicits._

import TableDdlStatement._
    
/** PostgreSQL ALTER TABLE operations */
// http://www.postgresql.org/docs/current/static/sql-altertable.html
object PostgresqlTableDdlStatement {
    case class ChangeType(newType: DataType) extends AlterColumnOperation
}

import PostgresqlTableDdlStatement._

class PostgresqlScriptSerializer(context: Context) extends ScriptSerializer(context) {
    import context._
    
    override def serializeAlterColumnOperation(op: AlterColumnOperation) = op match {
        case ChangeType(dt) => "TYPE " + serializeDataType(dt)
        case _ => super.serializeAlterColumnOperation(op)
    }
}

// vim: set ts=4 sw=4 et:
