package ru.yandex.mysqlDiff.script

import model._

import Implicits._

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
}

/** Comment including comment markers */
case class CommentElement(comment: String) extends ScriptElement

abstract class ScriptStatement extends ScriptElement

/** Output as in into the result text */
case class Unparsed(q: String) extends ScriptElement

abstract class DdlStatement extends ScriptStatement

abstract class TableDdlStatement(name: String) extends DdlStatement

// http://dev.mysql.com/doc/refman/5.1/en/create-table.html
/*
case class CreateTableStatement(name: String, ifNotExists: Boolean,
        entries: Seq[TableDdlStatement.TableElement], options: Seq[TableOption])
    extends TableDdlStatement(name)
{
    def this(name: String, ifNotExists: Boolean, entries: Seq[TableDdlStatement.TableElement]) =
        this(name, ifNotExists, entries, Nil)

    import TableDdlStatement._
    
}
*/

// options is for MySQL
case class CreateTableStatement(name: String, ifNotExists: Boolean,
        contentsSource: TableDdlStatement.TableContentsSource, options: Seq[TableOption])
    extends TableDdlStatement(name)
{
    import TableDdlStatement._
    
    def elements: Seq[TableElement] = contentsSource match {
        case TableElementList(seq) => seq
    }
    
    def columns: Seq[Column] = elements.flatMap { case c: Column => Some(c); case _ => None }
    def indexes: Seq[Index] = elements.flatMap { case c: Index => Some(c); case _=> None }
    def uniqueKeys: Seq[UniqueKey] = elements.flatMap { case c: UniqueKey => Some(c); case _ => None }
    def foreignKeys: Seq[ForeignKey] = elements.flatMap { case c: ForeignKey => Some(c); case _ => None }
    def primaryKeys: Seq[PrimaryKey] = elements.flatMap { case c: PrimaryKey => Some(c); case _ => None }
    
    def column(name: String) = columns.find(_.name == name).get
}

object TableDdlStatement {
    abstract class TableContentsSource
    case class TableElementList(elements: Seq[TableElement]) extends TableContentsSource
    
    /** Object inside table, XXX: rename to TableElement */
    abstract class TableElement
    
    // XXX: MySQL-specific
    abstract class ColumnPosition
    case class ColumnAfter(name: String) extends ColumnPosition
    case object ColumnFirst extends ColumnPosition
    
    /** Something can be specified with column */
    abstract class ColumnPropertyDecl
    
    case class References(table: String, columns: Seq[String],
            updateRule: Option[ImportedKeyRule], deleteRule: Option[ImportedKeyRule])
    
    /** Either column property */
    case class ModelColumnProperty(columnProperty: ColumnProperty) extends ColumnPropertyDecl
    
    // Or shortcuts
    case object InlinePrimaryKey extends ColumnPropertyDecl
    case object InlineUnique extends ColumnPropertyDecl
    case class InlineReferences(references: References)
        extends ColumnPropertyDecl
    {
        require(references.columns.length == 1)
    }
    
    case class Column(name: String, dataType: DataType, properties: Seq[ColumnPropertyDecl]) extends TableElement {
        def this(model: ColumnModel) =
            this(model.name, model.dataType, model.properties.properties.map(new ModelColumnProperty(_)))
        
        def modelProperties =
            properties.flatMap { case ModelColumnProperty(p) => Some(p); case _ => None }
        def defaultValue = modelProperties.defaultValue
        def isNotNull = modelProperties.isNotNull
        
        def addProperty(c: ColumnPropertyDecl) = new Column(name, dataType, properties ++ List(c))
    }
    
    /** Anithing except column */
    trait Extra extends TableElement
    
    /** CONSTRAINT */
    trait Constraint extends Extra
    
    case class Index(index: model.IndexModel) extends Extra
    
    case class UniqueKey(uk: model.UniqueKeyModel) extends Constraint
    
    case class PrimaryKey(pk: model.PrimaryKeyModel) extends Constraint
    
    case class ForeignKey(fk: model.ForeignKeyModel) extends Constraint
    
    case class Check(expr: Any) extends TableElement
    
    // like options skipped
    case class LikeClause(tableName: String) extends TableElement
    
    /** Operation on table */
    trait Operation
    
    trait DropOperation extends Operation
    trait ModifyOperation extends Operation
    
    trait ColumnOperation extends Operation
    trait ExtraOperation extends Operation
    
    abstract class AddSomething extends Operation
    case class AddExtra(e: Extra) extends AddSomething
    case class AddColumn(c: Column, position: Option[ColumnPosition]) extends AddSomething
    
    def AddPrimaryKey(pk: PrimaryKeyModel) = AddExtra(new PrimaryKey(pk))
    def AddForeignKey(fk: ForeignKeyModel) = AddExtra(new ForeignKey(fk))
    def AddUniqueKey(un: UniqueKeyModel) = AddExtra(new UniqueKey(un))
    def AddIndex(index: IndexModel) = AddExtra(new Index(index))
    
    case class ChangeColumn(oldName: String, model: ColumnModel, position: Option[ColumnPosition])
        extends ModifyOperation with ColumnOperation
    
    case class ModifyColumn(model: ColumnModel, position: Option[ColumnPosition])
        extends ModifyOperation with ColumnOperation
    
    // column operation
    trait AlterColumnOperation
    
    /** @param value None means DROP DEFAULT */
    case class SetDefault(value: Option[SqlValue]) extends AlterColumnOperation
    
    case class AlterColumn(name: String, op: AlterColumnOperation)
        extends ModifyOperation with ColumnOperation
    
    case class DropColumn(name: String) extends DropOperation with ColumnOperation
    
    case object DropPrimaryKey extends DropOperation with ExtraOperation
    
    case class DropIndex(name: String) extends DropOperation with ExtraOperation
    
    case class DropUniqueKey(name: String) extends DropOperation with ExtraOperation
    
    case class DropForeignKey(name: String) extends DropOperation with ExtraOperation
    
    case class ChangeTableOption(o: model.TableOption) extends Operation
    
    case object EnableKeys extends Operation
    case object DisableKeys extends Operation
    
    case class Rename(newName: String) extends Operation
    
    case class OrderBy(colNames: Seq[String]) extends Operation
}

case class DropTableStatement(name: String, ifExists: Boolean) extends TableDdlStatement(name)

case class RenameTableStatement(name: String, newName: String) extends TableDdlStatement(name)

case class AlterTableStatement(name: String, ops: Seq[TableDdlStatement.Operation])
        extends TableDdlStatement(name)
{
    require(ops.length > 0, "AlterTableStatement must have ops")
    
    def this(name: String, op: TableDdlStatement.Operation) = this(name, List(op))
    
    def flatten = ops.map(new AlterTableStatement(name, _))
}


abstract class ViewDdlStatement(name: String) extends DdlStatement

case class CreateViewStatement(name: String, select: SelectStatement) extends ViewDdlStatement(name)

case class DropViewStatement(name: String) extends ViewDdlStatement(name)


abstract class DmlStatement extends ScriptStatement

case class InsertStatement(table: String, ignore: Boolean,
        columns: Option[Seq[String]], data: Seq[Seq[SqlExpr]])
    extends DmlStatement
{
    require(table.length > 0)
    
    require(columns.isEmpty || !columns.get.isEmpty)
    require(!data.isEmpty)
    
    val columnsCount = columns.map(_.length).getOrElse(data.first.length)
    
    require(data.forall(columnsCount == _.length))
}

case class UpdateStatement(table: String, set: Seq[(String, SqlExpr)], condition: Option[SqlExpr])
    extends DmlStatement
{

}

case class DeleteStatement(table: String, condition: Option[SqlExpr])
    extends DmlStatement

case class SelectStatement(expr: Seq[SqlExpr], tables: Seq[String], condition: Option[SqlExpr])
    extends DmlStatement


abstract class SqlExpr

case class CastExpr(expr: SqlExpr, as: DataType) extends SqlExpr

case object SelectStar extends SqlExpr

case class NameExpr(name: String) extends SqlExpr
case class BinaryOpExpr(left: SqlExpr, op: String, right: SqlExpr) extends SqlExpr

case class VariableExpr(name: String) extends SqlExpr

object ScriptTests extends org.specs.Specification {
}

// vim: set ts=4 sw=4 et:
