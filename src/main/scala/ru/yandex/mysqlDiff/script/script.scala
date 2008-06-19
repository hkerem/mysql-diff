package ru.yandex.mysqlDiff.script

import model._

class Script(val stmts: Seq[ScriptElement]) {
    def statements = stmts.flatMap {
        case s: ScriptStatement => Some(s)
        case _ => None
    }
    
    def ddlStatements = statements.flatMap {
        case s: DdlStatement => Some(s)
        case _ => None
    }
    
    def dmlStatements = statements.flatMap {
        case s: DmlStatement => Some(s)
        case _ => None
    }
}

abstract class ScriptElement

/** Comment including comment markers */
case class CommentElement(comment: String) extends ScriptElement

abstract class ScriptStatement extends ScriptElement

case class Unparsed(q: String) extends ScriptElement

abstract class DdlStatement extends ScriptStatement

case class CreateTableStatement(name: String, ifNotExists: Boolean,
        entries: Seq[CreateTableStatement.Entry], options: Seq[TableOption])
    extends DdlStatement
{
    def this(name: String, ifNotExists: Boolean, entries: Seq[CreateTableStatement.Entry]) =
        this(name, ifNotExists, entries, Nil)

    import CreateTableStatement._
    
    def columns: Seq[Column] = entries.flatMap {
        case c: Column => Some(c)
        case _ => None
    }
    
    def column(name: String) = columns.find(_.name == name).get
}

object CreateTableStatement {
    abstract class Entry
    
    case class Column(name: String, dataType: DataType, properties: ColumnProperties) extends Entry {
        def defaultValue = properties.defaultValue
        def isNotNull = properties.isNotNull
    }
    
    case class Index(index: model.IndexModel) extends Entry
    
    case class PrimaryKey(pk: model.PrimaryKey) extends Entry
}

case class DropTableStatement(name: String) extends DdlStatement

case class RenameTableStatement(oldName: String, newName: String) extends DdlStatement

case class AlterTableStatement(tableName: String, ops: Seq[AlterTableStatement.Operation])
        extends DdlStatement
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

abstract class DmlStatement extends ScriptStatement

case class InsertStatement(table: String, ignore: Boolean,
        columns: Option[Seq[String]], data: Seq[Seq[SqlValue]])
    extends DmlStatement
{
    require(table.length > 0)
    
    require(columns.isEmpty || !columns.get.isEmpty)
    require(!data.isEmpty)
    
    val columnsCount = columns.map(_.length).getOrElse(data.first.length)
    
    require(data.forall(columnsCount == _.length))
}

object ScriptTests extends org.specs.Specification {
}

// vim: set ts=4 sw=4 et:
