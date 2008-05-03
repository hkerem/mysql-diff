package ru.yandex.mysqlDiff.script

import model._

class Script(val stmts: Seq[ScriptStatement])

abstract class ScriptStatement

case class CreateTableStatement(model: TableModel) extends ScriptStatement

case class UnparsedStatement(q: String) extends ScriptStatement

case class AlterTableStatement(tableName: String, op: AlterTableStatement.Operation)

object AlterTableStatement {
    abstract class Operation
    
    case class AddColumn(model: ColumnModel) extends Operation
    
    case class ChangeColumn(oldName: String, model: ColumnModel) extends Operation
    
    case class ModifyColumn(model: ColumnModel) extends Operation
}

// vim: set ts=4 sw=4 et:
