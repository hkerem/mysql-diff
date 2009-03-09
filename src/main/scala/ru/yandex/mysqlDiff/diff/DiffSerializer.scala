package ru.yandex.mysqlDiff.diff

import model._
import script._

/**
 * Serialize diff model to script.
 */
class DiffSerializer(val context: Context) {
    import context._
    
    def alterColumnScript(cd: ColumnDiff, table: TableModel) =
        AlterTableStatement(table.name, List(alterColumnStmt(cd, table)))
    
    def alterColumnStmt(cd: ColumnDiff, table: TableModel) = cd match {
        case CreateColumnDiff(c) => TableDdlStatement.AddColumn(c)
        case DropColumnDiff(name) => TableDdlStatement.DropColumn(name)
        case ChangeColumnDiff(name, Some(newName), diff) =>
                TableDdlStatement.ChangeColumn(name, table.column(newName))
        case ChangeColumnDiff(name, None, diff) =>
                TableDdlStatement.ModifyColumn(table.column(name))
    }
    
    def dropExtraStmt(k: TableExtra) = k match {
        case i: IndexModel => TableDdlStatement.DropIndex(i.name.get)
        case _: PrimaryKeyModel => TableDdlStatement.DropPrimaryKey
        case u: UniqueKeyModel => TableDdlStatement.DropUniqueKey(u.name.get)
        case f: ForeignKeyModel => TableDdlStatement.DropForeignKey(f.name.get)
    }
    
    def createExtraStmt(k: TableExtra) = k match {
        case i: IndexModel => TableDdlStatement.AddIndex(i)
        case u: UniqueKeyModel => TableDdlStatement.AddUniqueKey(u)
        case p: PrimaryKeyModel => TableDdlStatement.AddPrimaryKey(p)
        case f: ForeignKeyModel => TableDdlStatement.AddForeignKey(f)
    }
    
    def alterExtraStmts(d: ExtraDiff) = d match {
        case DropExtraDiff(k) => List(dropExtraStmt(k))
        case CreateExtraDiff(k) => List(createExtraStmt(k))
        case ChangeExtraDiff(ok, nk) => List(dropExtraStmt(ok), createExtraStmt(nk))
    }
    
    def alterTableOptionStmt(od: TableOptionDiff, table: TableModel) = {
        val o = od match {
            case CreateTableOptionDiff(o) => o
            case ChangeTableOptionDiff(o, n) => n
        }
        TableDdlStatement.ChangeTableOption(o)
    }
    
    def alterExtraScript(d: ExtraDiff, table: TableModel) =
        AlterTableStatement(table.name, alterExtraStmts(d))
    
    def alterTableEntryStmts(d: TableEntryDiff, table: TableModel) = d match {
        case kd: ExtraDiff => alterExtraStmts(kd)
        case cd: ColumnDiff => List(alterColumnStmt(cd, table))
        case od: TableOptionDiff => List(alterTableOptionStmt(od, table))
    }
    
    private def operationOrder(op: TableDdlStatement.Operation) = op match {
        case _: TableDdlStatement.DropOperation => op match {
            case _: TableDdlStatement.ExtraOperation => 11
            case _: TableDdlStatement.ColumnOperation => 12
            case _ => 13
        }
        case _: TableDdlStatement.ModifyOperation => 20
        case TableDdlStatement.AddEntry(e) => e match {
            case _: TableDdlStatement.Column => 31
            case _ => 32
        }
        case _ => 99
    }
    
    /**
     * Does not include rename.
     * @param model new model
     */
    def alterScript(diff: ChangeTableDiff, table: TableModel): Seq[ScriptElement] = {

        List[ScriptElement](CommentElement("-- " + diff.toString)) ++
        {
            // for CAP-101
            // XXX: simplify
            val (addPks, rest) = diff.entriesDiff.toList.partition {
                case CreateExtraDiff(_: PrimaryKeyModel) => true
                case _ => false
            }
            
            require(addPks.length <= 1)
            
            val addPk: Option[PrimaryKeyModel] =
                addPks.firstOption.map { case CreateExtraDiff(pk: PrimaryKeyModel) => pk }
            val addPkSingleColumn = addPk.filter(_.columns.length == 1).map(_.columns.first)
            
            val (addPkSingleColumnDiffs, rest2) = rest.partition {
                case CreateColumnDiff(c) if Some(c.name) == addPkSingleColumn => true
                case _ => false
            }
            
            require(addPkSingleColumnDiffs.length <= 1)
            
            val addPkSingleColumnDiffProper = addPkSingleColumnDiffs.firstOption
                    .map(x => new TableDdlStatement.AddEntry(new TableDdlStatement.Column(x.asInstanceOf[CreateColumnDiff].column) addProperty TableDdlStatement.InlinePrimaryKey))
            
            // end of CAP-101
            
            import TableDdlStatement._
            val ops = rest2.flatMap(alterTableEntryStmts(_, table)) ++ addPkSingleColumnDiffProper
            val sorted = scala.util.Sorting.stableSort(ops, operationOrder _)
            // XXX: make configurable
            if (false) sorted.map { op: TableDdlStatement.Operation => AlterTableStatement(table.name, List(op)) } 
            else List(AlterTableStatement(table.name, sorted))
        }
        
        // XXX: sort: drop, then change, then create
    }
    
    def serializeCreateTableDiff(c: CreateTableDiff) =
        modelSerializer.serializeTable(c.table) :: Nil
    
    def serializeDropTableDiff(d: DropTableDiff) =
        DropTableStatement(d.name, false) :: Nil
    
    def serializeChangeTableDiff(d: ChangeTableDiff, newTable: TableModel)
        : Seq[ScriptElement] =
    {
        val ChangeTableDiff(name, renameTo, columnDiff, indexDiff, optionDiff) = d
        require(renameTo.isEmpty || renameTo.get == newTable.name)
        renameTo.map(RenameTableStatement(name, _)).toSeq ++
                alterScript(d, newTable)
    }
    
    // XXX: rename to serialize
    def serializeToScript(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[ScriptElement] =
    {
        val newTablesMap: Map[String, TableModel] = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: Map[String, TableModel] = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        diff.tableDiff.flatMap(_ match {
            case c: CreateTableDiff => serializeCreateTableDiff(c)
            case d: DropTableDiff => serializeDropTableDiff(d)
            case d @ ChangeTableDiff(name, renameTo, columnDiff, indexDiff, optionDiff) =>
                    serializeChangeTableDiff(d, newTablesMap(d.newName))
        })
    }
    
    def serializeToScriptStrings(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[String] =
        serializeToScript(diff, oldModel, newModel).map(scriptSerializer.serialize(_))
    
    // XXX: rename to serializeToText
    def serialize(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): String = {
        val options = ScriptSerializer.Options.multiline
        scriptSerializer.serialize(serializeToScript(diff, oldModel, newModel), options)
    }
}

object DiffSerializerTests extends org.specs.Specification {
    
    import AlterTableStatement._
    import CreateTableStatement._

    // XXX: import basic context
    import Environment.defaultContext._
    import diffSerializer._
    
    import TableDdlStatement._
    
    /** Partial function to predicate */
    private def pftp[T](f: PartialFunction[T, Any]) =
        (t: T) => f.isDefinedAt(t)
    
    "DROP before ADD" in {
        val oldTable = modelParser.parseCreateTableScript("CREATE TABLE users (id INT, name VARCHAR(100), dep_id INT, INDEX ni(name))")
        val newTable = modelParser.parseCreateTableScript("CREATE TABLE users (id INT, login VARCHAR(10), dep_id BIGINT, INDEX li(login))")
        val diff = diffMaker.compareTables(oldTable, newTable).get
        val script = alterScript(diff, newTable).filter(_.isInstanceOf[ScriptStatement])
        
        script.size must_== 1
        script(0) must beLike { case AlterTableStatement(_, _) => true }
        val altStmt = script(0).asInstanceOf[AlterTableStatement].ops

        val dropNameI = altStmt.findIndexOf(
            pftp { case DropColumn("name") => true })
        
        val dropNiI = altStmt.findIndexOf(
            pftp { case DropIndex("ni") => true })
        
        val addLoginI = altStmt.findIndexOf(
            pftp { case AddEntry(c: Column) if c.name == "login" => true })
        
        val addLiI = altStmt.findIndexOf(
            pftp { case AddEntry(Index(c: IndexModel)) if c.name == Some("li") => true })
        
        dropNiI must be_>=(0)
        dropNameI must be_>=(0)
        addLoginI must be_>=(0)
        addLiI must be_>=(0)
        
        // drop index .. drop column .. add column .. add index
        dropNiI must be_<(dropNameI)
        dropNameI must be_<(addLoginI)
        addLoginI must be_<(addLiI)
    }


}

// vim: set ts=4 sw=4 et:
