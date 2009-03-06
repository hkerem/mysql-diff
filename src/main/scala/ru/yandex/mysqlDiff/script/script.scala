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

abstract class ScriptElement {
    /** Shortcut */
    def serialize = ScriptSerializer.serialize(this)
    def toText = serialize
}

/** Comment including comment markers */
case class CommentElement(comment: String) extends ScriptElement

abstract class ScriptStatement extends ScriptElement

case class Unparsed(q: String) extends ScriptElement

abstract class DdlStatement extends ScriptStatement

abstract class TableDdlStatement(name: String) extends DdlStatement

// http://dev.mysql.com/doc/refman/5.1/en/create-table.html
case class CreateTableStatement(name: String, ifNotExists: Boolean,
        entries: Seq[CreateTableStatement.Entry], options: Seq[TableOption])
    extends TableDdlStatement(name)
{
    def this(name: String, ifNotExists: Boolean, entries: Seq[CreateTableStatement.Entry]) =
        this(name, ifNotExists, entries, Nil)

    import CreateTableStatement._
    
    def columns: Seq[Column] = entries.flatMap { case c: Column => Some(c); case _ => None }
    def indexes: Seq[Index] = entries.flatMap { case c: Index => Some(c); case _=> None }
    def fks: Seq[ForeignKey] = entries.flatMap { case c: ForeignKey => Some(c); case _ => None }
    
    def column(name: String) = columns.find(_.name == name).get
}

// http://dev.mysql.com/doc/refman/5.1/en/create-table.html
case class CreateTableLikeStatement(name: String, ifNotExists: Boolean, likeWhat: String)
    extends TableDdlStatement(name)
{
}

object CreateTableStatement {
    abstract class Entry
    
    abstract class ColumnPropertyDecl
    
    case class ModelColumnProperty(columnProperty: ColumnProperty) extends ColumnPropertyDecl
    
    case object InlinePrimaryKey extends ColumnPropertyDecl
    case object InlineUnique extends ColumnPropertyDecl
    case class InlineReferences(tableName: String, columnName: String) extends ColumnPropertyDecl
    
    case class Column(name: String, dataType: DataType, properties: Seq[ColumnPropertyDecl]) extends Entry {
        def this(model: ColumnModel) =
            this(model.name, model.dataType, model.properties.properties.map(new ModelColumnProperty(_)))
        
        def modelProperties =
            ColumnProperties(properties.flatMap { case ModelColumnProperty(p) => Some(p); case _ => None })
        def defaultValue = modelProperties.defaultValue
        def isNotNull = modelProperties.isNotNull
        
        def addProperty(c: ColumnPropertyDecl) = new Column(name, dataType, properties ++ List(c))
    }
    
    case class Index(index: model.IndexModel) extends Entry
    
    case class PrimaryKey(pk: model.PrimaryKeyModel) extends Entry
    
    case class ForeignKey(fk: model.ForeignKeyModel) extends Entry
}

case class DropTableStatement(name: String, ifExists: Boolean) extends TableDdlStatement(name)

case class RenameTableStatement(name: String, newName: String) extends TableDdlStatement(name)

case class AlterTableStatement(name: String, ops: Seq[AlterTableStatement.Operation])
        extends TableDdlStatement(name)
{
    require(ops.length > 0)
    
    def this(name: String, op: AlterTableStatement.Operation) = this(name, List(op))
    
    def flatten = ops.map(new AlterTableStatement(name, _))
}

object AlterTableStatement {
    import script.{CreateTableStatement => cts}
    
    trait Operation
    
    trait DropOperation extends Operation
    trait AddOperation extends Operation
    trait ModifyOperation extends Operation
    
    trait ColumnOperation extends Operation
    trait KeyOperation extends Operation
    
    case class AddColumn(column: cts.Column) extends AddOperation with ColumnOperation {
        def this(model: ColumnModel) = this(new cts.Column(model))
    }
    
    case class ChangeColumn(oldName: String, model: ColumnModel) extends ModifyOperation with ColumnOperation
    
    case class ModifyColumn(model: ColumnModel) extends ModifyOperation with ColumnOperation
    
    case class DropColumn(name: String) extends DropOperation with ColumnOperation
    
    case class AddPrimaryKey(pk: PrimaryKeyModel) extends AddOperation with KeyOperation
    
    case object DropPrimaryKey extends DropOperation with KeyOperation
    
    case class AddIndex(id: IndexModel) extends AddOperation with KeyOperation
    
    case class DropIndex(name: String) extends DropOperation with KeyOperation
    
    case class DropForeignKey(name: String) extends DropOperation with KeyOperation
    
    case class AddForeignKey(fk: ForeignKeyModel) extends AddOperation with KeyOperation
    
    // XXX: rename to ChangeTableOption
    case class TableOption(o: model.TableOption) extends Operation
}

abstract class ViewDdlStatement(name: String) extends DdlStatement

case class CreateViewStatement(name: String, select: SelectStatement) extends ViewDdlStatement(name)

case class DropViewStatement(name: String) extends ViewDdlStatement(name)


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

// both expression and condition
abstract class SelectExpr

case object SelectStar extends SelectExpr
case class SelectName(name: String) extends SelectExpr
case class SelectValue(value: SqlValue) extends SelectExpr
case class SelectBinary(left: SelectExpr, op: String, right: SelectExpr) extends SelectExpr

case class SelectStatement(expr: Seq[SelectExpr], tables: Seq[String], condition: Option[SelectExpr])

object ScriptTests extends org.specs.Specification {
}

// vim: set ts=4 sw=4 et:
