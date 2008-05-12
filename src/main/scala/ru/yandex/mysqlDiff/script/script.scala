package ru.yandex.mysqlDiff.script

import model._

class Script(val stmts: Seq[ScriptElement])

abstract class ScriptElement

/** Comment including comment markers */
case class CommentElement(comment: String) extends ScriptElement

abstract class ScriptStatement extends ScriptElement

case class Unparsed(q: String) extends ScriptElement

case class CreateTableStatement2(name: String, ifNotExists: Boolean, entries: Seq[CreateTableStatement.Entry])
    extends ScriptStatement

object CreateTableStatement {
    abstract class Entry
    
    abstract class ColumnProperty
    
    case class Nullable(nullable: Boolean) extends ColumnProperty
    
    case class DefaultValue(value: SqlValue) extends ColumnProperty
    
    case object AutoIncrement extends ColumnProperty
    
    case class Column(name: String, dataType: DataType, attrs: Seq[ColumnProperty]) extends Entry
    
    case class Index(index: model.IndexModel) extends Entry
    
    case class PrimaryKey(pk: model.PrimaryKey) extends Entry
}

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
    
    case class AddIndex(id: IndexModel) extends Operation
    
    case class DropIndex(name: String) extends Operation
}

// vim: set ts=4 sw=4 et:
