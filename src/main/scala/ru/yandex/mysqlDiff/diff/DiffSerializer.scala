package ru.yandex.mysqlDiff.diff

import model._
import script._

import Implicits._

/**
 * Serialize diff model to script.
 */
class DiffSerializer(val context: Context) {
    import context._
    
    import TableDdlStatement._
    
    def alterColumnScript(cd: ColumnDiff, table: TableModel) =
        AlterTableStatement(table.name, List(alterColumnStmt(cd, table)))
    
    def alterColumnStmt(cd: ColumnDiff, table: TableModel) = cd match {
        // XXX: position
        case CreateColumnDiff(ColumnModel(name, dataType, properties)) =>
            TableDdlStatement.AddColumn(
                new Column(name, dataType, properties.properties.map(p => new ModelColumnProperty(p))), None)
        case DropColumnDiff(name) => TableDdlStatement.DropColumn(name)
        case ChangeColumnDiff(name, Some(newName), diff) =>
                TableDdlStatement.ChangeColumn(name, table.column(newName), None)
        case ChangeColumnDiff(name, None, diff) =>
                TableDdlStatement.ModifyColumn(table.column(name), None)
    }
    
    def dropExtraStmt(k: TableExtra) = k match {
        case i: IndexModel =>
            TableDdlStatement.DropIndex(i.name.getOrThrow("cannot drop unnamed index"))
        case _: PrimaryKeyModel =>
            TableDdlStatement.DropPrimaryKey
        case u: UniqueKeyModel =>
            TableDdlStatement.DropUniqueKey(u.name.getOrThrow("cannot drop unnamed unique key"))
        case f: ForeignKeyModel =>
            TableDdlStatement.DropForeignKey(f.name.getOrThrow("cannot drop unnamed foreign key"))
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
            // FOREIGN must be dropped first
            case _: TableDdlStatement.DropForeignKey => 11
            // COLUMN must be dropped last
            case _: TableDdlStatement.ColumnOperation => 15
            case _ => 13
        }
        
        case _: TableDdlStatement.ModifyOperation => 20
        
        case _: TableDdlStatement.AddSomething => op match {
            // COLUMN must be added first
            case _: TableDdlStatement.AddColumn => 31
            // FOREIGN KEY must be added last
            case TableDdlStatement.AddExtra(_: ForeignKeyModel) => 35
            case _: TableDdlStatement.AddSomething => 33
            case _ => 33
        }
        
        case _ => 99
    }
    
    /**
     * Does not include rename.
     * @param model new model
     */
    def alterScript(diff: ChangeTableDiff, table: TableModel): Seq[ScriptElement] = {

        if (diff.entriesDiff.isEmpty)
            Seq()
        else {
            // XXX: simplify
            
            val addPks = diff.entriesDiff.toList.filter {
                // single column only
                case CreateExtraDiff(PrimaryKeyModel(_, Seq(_))) => true
                case _ => false
            }
            
            require(addPks.length <= 1)
            
            val addPk: Option[PrimaryKeyModel] =
                addPks.firstOption.map { case CreateExtraDiff(pk: PrimaryKeyModel) => pk }
            val addPkSingleColumn = addPk.map(_.columns.first).filter(cn => diff.entriesDiff.exists {
                    case CreateColumnDiff(c) if c.name == cn => true
                    case _ => false
                })
            
            val (addPkSingleColumnDiffs0, rest2) = diff.entriesDiff.toList.partition {
                case CreateColumnDiff(c) if Some(c.name) == addPkSingleColumn => true
                case CreateExtraDiff(PrimaryKeyModel(_, Seq(c))) if Some(c) == addPkSingleColumn => true
                case _ => false
            }
            
            val addPkSingleColumnDiffs = addPkSingleColumnDiffs0.filter {
                case CreateColumnDiff(_) => true
                case CreateExtraDiff(PrimaryKeyModel(_, Seq(_))) => false
            }
            
            require(addPkSingleColumnDiffs.length <= 1)
            
            val addPkSingleColumnDiffProper = addPkSingleColumnDiffs.firstOption
                    .map(x => new TableDdlStatement.AddColumn(new TableDdlStatement.Column(x.asInstanceOf[CreateColumnDiff].column) addProperty TableDdlStatement.InlinePrimaryKey, None))
            
            import TableDdlStatement._
            val ops = rest2.flatMap(alterTableEntryStmts(_, table)) ++ addPkSingleColumnDiffProper
            val sorted = scala.util.Sorting.stableSort(ops, operationOrder _)
            // XXX: make configurable
            if (sorted.isEmpty) Seq()
            else {
                if (false) sorted.map {
                    op: TableDdlStatement.Operation => AlterTableStatement(table.name, List(op)) }
                else Seq(AlterTableStatement(table.name, sorted))
            }
        }
        
    }
    
    def serializeChangeTableDiff(d: ChangeTableDiff, newTable: TableModel)
        : Seq[ScriptElement] =
    {
        serializeChangeTableDiffs(Seq((d, newTable)))
    }
    
    def serializeChangeTableDiffs(diff: Seq[(TableDiff, TableModel)]) = {
        val rename = new ArrayBuffer[ScriptElement]
        
        val dropFks = new ArrayBuffer[ScriptElement]
        val dropTables = new ArrayBuffer[ScriptElement]
        val drop = new ArrayBuffer[ScriptElement]
        val change = new ArrayBuffer[ScriptElement]
        val create = new ArrayBuffer[ScriptElement]
        val createTables = new ArrayBuffer[ScriptElement]
        val addFks = new ArrayBuffer[ScriptElement]
        
        for ((d, newTable) <- diff) {
            d match {
                case d @ ChangeTableDiff(name, renameTo, columnDiff, indexDiff, optionDiff) =>
                    require(renameTo.isEmpty || renameTo.get == newTable.name)
                    if (renameTo.isDefined)
                        rename += RenameTableStatement(name, renameTo.get)
                    
                    val (drFk, dr, ch, cr, crFk) = d.splitForOrder
                    dropFks ++= alterScript(drFk, newTable)
                    drop ++= alterScript(dr, newTable)
                    change ++= alterScript(ch, newTable)
                    create ++= alterScript(cr, newTable)
                    addFks ++= alterScript(crFk, newTable)
                case DropTableDiff(table) =>
                    dropTables += DropTableStatement(table.name, false)
                    if (!table.foreignKeys.isEmpty)
                        dropFks += AlterTableStatement(table.name, table.foreignKeys.map(fk =>
                                TableDdlStatement.DropForeignKey(fk.name.getOrThrow("cannot get unnamed key"))))
                case CreateTableDiff(table) =>
                    createTables += modelSerializer.serializeTable(table.dropForeignKeys)
                    if (!table.foreignKeys.isEmpty)
                        addFks += AlterTableStatement(table.name, table.foreignKeys.map(fk => TableDdlStatement.AddForeignKey(fk)))
            }
        }
        Seq[ScriptElement]() ++ rename ++ dropFks ++ dropTables ++ drop ++ change ++ create ++ createTables ++ addFks
    }
    
    def serializeCreateTableDiff(c: CreateTableDiff) =
        Seq(modelSerializer.serializeTable(c.table))
    
    def serializeDropTableDiff(d: DropTableDiff) =
        Seq(DropTableStatement(d.table.name, false))
    
    def serializeTableDiff(diff: TableDiff, newTable: TableModel) = diff match {
        case c: CreateTableDiff => serializeCreateTableDiff(c)
        case d: DropTableDiff => serializeDropTableDiff(d)
        case d: ChangeTableDiff => serializeChangeTableDiff(d, newTable)
    }
    
    // XXX: rename to serialize
    def serializeToScript(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[ScriptElement] =
    {
        val newTablesMap: Map[String, TableModel] = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: Map[String, TableModel] = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        
        def table(d: TableDiff) = d match {
            case d: ChangeTableDiff => newTablesMap(d.newName)
            case DropTableDiff(t) => t
            case CreateTableDiff(t) => t
        }
        
        serializeChangeTableDiffs(diff.tableDiff.map(d => (d, table(d))))
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
            pftp { case AddColumn(c: Column, _) if c.name == "login" => true })
        
        val addLiI = altStmt.findIndexOf(
            pftp { case AddExtra(Index(c: IndexModel)) if c.name == Some("li") => true })
        
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
