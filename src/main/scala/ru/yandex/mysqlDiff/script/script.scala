package ru.yandex.mysqlDiff.script

import model._

class Script(val stmts: Seq[ScriptElement])

abstract class ScriptElement

/** Comment including comment markers */
case class CommentElement(comment: String) extends ScriptElement

abstract class ScriptStatement extends ScriptElement

case class Unparsed(q: String) extends ScriptElement

case class CreateTableStatement(table: TableModel) extends ScriptStatement

case class DropTableStatement(name: String) extends ScriptStatement

case class RenameTableStatement(oldName: String, newName: String) extends ScriptStatement

case class AlterTableStatement(tableName: String, ops: Seq[AlterTableStatement.Operation])
        extends ScriptStatement
{
    require(ops.length > 0)
    
    def this(tableName: String, op: AlterTableStatement.Operation) = this(tableName, List(op))
    
    def flatten = ops.map(new AlterTableStatement(tableName, _))
}

object AlterTableStatement {
    abstract class Operation
    
    case class AddColumn(model: ColumnModel) extends Operation
    
    case class ChangeColumn(oldName: String, model: ColumnModel) extends Operation
    
    case class ModifyColumn(model: ColumnModel) extends Operation
    
    case class DropColumn(name: String) extends Operation
    
    case class AddPrimaryKey(pk: PrimaryKey) extends Operation
    
    case object DropPrimaryKey extends Operation
}

// vim: set ts=4 sw=4 et:
