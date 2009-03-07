package ru.yandex.mysqlDiff.diff

import model._
import script._

/**
 * Serialize diff model to script.
 */
class DiffSerializer(val context: Context) {
    import context._
    
    import script.{AlterTableStatement => ats}
    import script.{CreateTableStatement => cts}

    def alterColumnScript(cd: ColumnDiff, table: TableModel) =
        AlterTableStatement(table.name, List(alterColumnStmt(cd, table)))
    
    def alterColumnStmt(cd: ColumnDiff, table: TableModel) = cd match {
        case CreateColumnDiff(c) => new ats.AddColumn(c)
        case DropColumnDiff(name) => ats.DropColumn(name)
        case ChangeColumnDiff(name, Some(newName), diff) =>
                ats.ChangeColumn(name, table.column(newName))
        case ChangeColumnDiff(name, None, diff) =>
                ats.ModifyColumn(table.column(name))
    }
    
    def dropKeyStmt(k: KeyModel) = k match {
        case _: PrimaryKeyModel => ats.DropPrimaryKey
        case i: IndexModel => ats.DropIndex(i.name.get)
        case f: ForeignKeyModel => ats.DropForeignKey(f.name.get)
    }
    
    def createKeyStmt(k: KeyModel) = k match {
        case p: PrimaryKeyModel => ats.AddPrimaryKey(p)
        case i: IndexModel => ats.AddIndex(i)
        case f: ForeignKeyModel => ats.AddForeignKey(f)
    }
    
    def alterKeyStmts(d: KeyDiff) = d match {
        case DropKeyDiff(k) => List(dropKeyStmt(k))
        case CreateKeyDiff(k) => List(createKeyStmt(k))
        case ChangeKeyDiff(ok, nk) => List(dropKeyStmt(ok), createKeyStmt(nk))
    }
    
    def alterTableOptionStmt(od: TableOptionDiff, table: TableModel) = {
        val o = od match {
            case CreateTableOptionDiff(o) => o
            case ChangeTableOptionDiff(o, n) => n
        }
        ats.ChangeTableOption(o)
    }
    
    def alterKeyScript(d: KeyDiff, table: TableModel) =
        AlterTableStatement(table.name, alterKeyStmts(d))
    
    def alterTableEntryStmts(d: TableEntryDiff, table: TableModel) = d match {
        case kd: KeyDiff => alterKeyStmts(kd)
        case cd: ColumnDiff => List(alterColumnStmt(cd, table))
        case od: TableOptionDiff => List(alterTableOptionStmt(od, table))
    }
    
    private def operationOrder(op: ats.Operation) = op match {
        case _: ats.DropOperation => op match {
            case _: ats.KeyOperation => 11
            case _: ats.ColumnOperation => 12
            case _ => 13
        }
        case _: ats.ModifyOperation => 20
        case _: ats.AddOperation => op match {
            case _: ats.ColumnOperation => 31
            case _: ats.KeyOperation => 32
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
            val (addPks, rest) = diff.entriesDiff.partition {
                case CreateKeyDiff(_: PrimaryKeyModel) => true
                case _ => false
            }
            
            require(addPks.length <= 1)
            
            val addPk = addPks.firstOption.map(_.asInstanceOf[CreateKeyDiff])
            val addPkSingleColumn = addPk.filter(_.index.columns.length == 1).map(_.index.columns.first)
            
            val (addPkSingleColumnDiffs, rest2) = rest.partition {
                case CreateColumnDiff(c) if Some(c.name) == addPkSingleColumn => true
                case _ => false
            }
            
            require(addPkSingleColumnDiffs.length <= 1)
            
            val addPkSingleColumnDiffProper = addPkSingleColumnDiffs.firstOption
                    .map(x => new ats.AddColumn(new cts.Column(x.asInstanceOf[CreateColumnDiff].column) addProperty cts.InlinePrimaryKey))
            
            // end of CAP-101
            
            import ats._
            val ops = rest2.flatMap(alterTableEntryStmts(_, table)) ++ addPkSingleColumnDiffProper
            val sorted = scala.util.Sorting.stableSort(ops, operationOrder _)
            // XXX: make configurable
            if (false) sorted.map { op: ats.Operation => AlterTableStatement(table.name, List(op)) } 
            else List(AlterTableStatement(table.name, sorted))
        }
        
        // XXX: sort: drop, then change, then create
    }
    
    def serializeCreateTableDiff(c: CreateTableDiff) =
        ModelSerializer.serializeTable(c.table) :: Nil
    
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
        serializeToScript(diff, oldModel, newModel).map(ScriptSerializer.serialize(_))
    
    // XXX: rename to serializeToText
    def serialize(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): String = {
        val options = ScriptSerializer.Options.multiline
        ScriptSerializer.serialize(serializeToScript(diff, oldModel, newModel), options)
    }
}

object DiffSerializerTests extends org.specs.Specification {
    
    import AlterTableStatement._
    import CreateTableStatement._

    import Environment.defaultContext._
    import diffSerializer._
    
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

        val dropNameI = altStmt.findIndexOf(pftp { case DropColumn("name") => true })
        val dropNiI = altStmt.findIndexOf(pftp { case DropIndex("ni") => true })
        val addLoginI = altStmt.findIndexOf(pftp { case AddColumn(c: Column) if c.name == "login" => true })
        val addLiI = altStmt.findIndexOf(pftp { case AddIndex(c: IndexModel) if c.name == Some("li") => true })
        
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
