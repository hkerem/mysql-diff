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
    
    protected def alterColumnScript(cd: ColumnDiff, table: TableModel) =
        alterColumnStmt(cd, table)
        //AlterTableStatement(table.name, List(alterColumnStmt(cd, table)))
    
    protected def changeColumnStmt(cd: ChangeColumnDiff, table: TableModel) =
        AlterTableStatement(table.name, List(cd match {
            case ChangeColumnDiff(name, Some(newName), diff) =>
                    TableDdlStatement.ChangeColumn(name, table.column(newName), None)
            case ChangeColumnDiff(name, None, diff) =>
                    TableDdlStatement.ModifyColumn(table.column(name), None)
        }))
    
    protected def alterColumnStmt(cd: ColumnDiff, table: TableModel): AlterTableStatement =
        cd match {
            // XXX: position
            case CreateColumnDiff(ColumnModel(name, dataType, properties)) =>
                AlterTableStatement(table.name, List(TableDdlStatement.AddColumn(
                    new Column(name, dataType, properties.properties.map(
                        p => new ModelColumnProperty(p))), None)))
            case DropColumnDiff(name) =>
                AlterTableStatement(table.name, List(TableDdlStatement.DropColumn(name)))
            case cd: ChangeColumnDiff => changeColumnStmt(cd, table)
            
            case CreateColumnWithInlinePrimaryKeyCommand(col, pk) =>
                AlterTableStatement(table.name, List(
                    new TableDdlStatement.AddColumn(
                        new TableDdlStatement.Column(col) addProperty TableDdlStatement.InlinePrimaryKey, None)))
        }
    
    protected def dropExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel =>
            DropIndexStatement(i.name.getOrThrow("cannot drop unnamed index"))
        case k =>
            AlterTableStatement(table.name, List(k match {
                    case _: PrimaryKeyModel =>
                        TableDdlStatement.DropPrimaryKey
                    case u: UniqueKeyModel =>
                        TableDdlStatement.DropUniqueKey(u.name.getOrThrow("cannot drop unnamed unique key"))
                    case f: ForeignKeyModel =>
                        TableDdlStatement.DropForeignKey(f.name.getOrThrow("cannot drop unnamed foreign key"))
                }))
    }
    
    protected def createExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel =>
            CreateIndexStatement(i.name.getOrThrow("cannot create unnamed index"), table.name, i.columns)
        case k =>
            AlterTableStatement(table.name, List(k match {
                case u: UniqueKeyModel => TableDdlStatement.AddUniqueKey(u)
                case p: PrimaryKeyModel => TableDdlStatement.AddPrimaryKey(p)
                case f: ForeignKeyModel => TableDdlStatement.AddForeignKey(f)
            }))
    }
    
    protected def alterExtraStmt(d: ExtraDiff, table: TableModel) = d match {
        case DropExtraDiff(k) => dropExtraStmt(k, table)
        case CreateExtraDiff(k) => createExtraStmt(k, table)
        //case ChangeExtraDiff(ok, nk) => List(dropExtraStmt(ok), createExtraStmt(nk))
        // XXX: split change -> drop, create
    }
    
    protected def alterTableOptionStmt(od: TableOptionDiff, table: TableModel) =
        AlterTableStatement(table.name, List({
                val o = od match {
                    case CreateTableOptionDiff(o) => o
                    case ChangeTableOptionDiff(o, n) => n
                }
                TableDdlStatement.ChangeTableOption(o)
            }))
    
    protected def alterExtraScript(d: ExtraDiff, table: TableModel) =
        alterExtraStmt(d, table)
        //AlterTableStatement(table.name, List(alterExtraStmt(d)))
    
    protected def alterTableEntryStmt(d: TableEntryDiff, table: TableModel) = d match {
        case kd: ExtraDiff => alterExtraStmt(kd, table)
        case cd: ColumnDiff => alterColumnStmt(cd, table)
        case od: TableOptionDiff => alterTableOptionStmt(od, table)
    }
    
    private def operationOrder(op: TableEntryDiff) = op match {
        case _: TableEntryDropDiff => op match {
            // FOREIGN must be dropped first
            //case _: TableEntry TableDdlStatement.DropForeignKey => 11
            // COLUMN must be dropped last
            case _: ColumnDiff => 15
            case _ => 13
        }
        
        case _: TableEntryChangeDiff => 20
        
        case _: TableEntryCreateDiff => op match {
            // COLUMN must be added first
            case _: ColumnDiff => 31
            // FOREIGN KEY must be added last
            //case TableDdlStatement.AddExtra(_: ForeignKeyModel) => 35
            //case _: TableDdlStatement.AddSomething => 33
            case _ => 33
        }
        
        case _ => 99
    }
    
    protected def mergeSingleColumnPks(entriesDiff: Seq[TableEntryDiff]) = {
        val (Seq(addPkSc @ _*), Seq(rest @ _*)) = entriesDiff.partition {
            case CreateExtraDiff(PrimaryKeyModel(_, Seq(colName)))
                if (entriesDiff.exists {
                    case CreateColumnDiff(c) if c.name == colName => true
                    case _ => false
                }) => true
            case _ => false
        }
        require(addPkSc.length <= 1)
        val (Seq(thatSingleColumnForPk @ _*), Seq(rest2 @ _*)) = rest.partition {
            case CreateColumnDiff(c)
                if (addPkSc.length > 0) &&
                    c.name == addPkSc.first.asInstanceOf[PrimaryKeyModel].columnNames.first => true
            case _ => false
        }
        require(addPkSc.length == thatSingleColumnForPk.length)
        (addPkSc.toList.zip(thatSingleColumnForPk.toList).map {
            case (CreateExtraDiff(pk: PrimaryKeyModel), CreateColumnDiff(col)) =>
                CreateColumnWithInlinePrimaryKeyCommand(col, pk)
        }) ++ rest2
    }
    
    // hook for overrides
    protected def adjustDiffHook(entriesDiff: Seq[TableEntryDiff]) =
        entriesDiff
    
    /**
     * Does not include rename.
     * @param model new model
     */
    def alterScript(diff: ChangeTableDiff, table: TableModel): Seq[ScriptElement] = {

        if (diff.entriesDiff.isEmpty)
            Seq()
        else {
            
            val entriesDiff = adjustDiffHook(mergeSingleColumnPks(diff.entriesDiff))
            
            val sorted = scala.util.Sorting.stableSort(entriesDiff, operationOrder _)
            val ops = sorted.map(alterTableEntryStmt(_, table))
            
            // XXX: make configurable
            if (ops.isEmpty) Seq()
            else {
                //if (false) ops.map {
                //    op: TableDdlStatement.Operation => AlterTableStatement(table.name, List(op)) }
                //else Seq(AlterTableStatement(table.name, ops))
                ops.foldLeft(List[DdlStatement]()) { (a, b) =>
                    a match {
                        case Seq() => a ++ List(b)
                        case l =>
                            def mergeIfPossible(a: DdlStatement, b: DdlStatement) = (a, b) match {
                                case (AlterTableStatement(name1, ops1), AlterTableStatement(name2, ops2)) =>
                                    require(name1 == name2)
                                    List(AlterTableStatement(name1, ops1 ++ ops2))
                                case (a, b) => List(a, b)
                            }
                            l.init ++ mergeIfPossible(l.last, b)
                    }
                }
            }
        }
        
    }
    
    def serializeChangeTableDiff(d: ChangeTableDiff, newTable: TableModel)
        : Seq[ScriptElement] =
    {
        serializeChangeTableDiffs(Seq((d, newTable)))
    }
    
    /**
     * @param diff sequence of pairs (diff, new model)
     */
    def serializeChangeTableDiffs(diff: Seq[(DatabaseDeclDiff, DatabaseDecl)]) = {
        val rename = new ArrayBuffer[ScriptElement]
        
        val dropFks = new ArrayBuffer[ScriptElement]
        val dropTables = new ArrayBuffer[ScriptElement]
        val drop = new ArrayBuffer[ScriptElement]
        val dropSequences = new ArrayBuffer[ScriptElement]
        val change = new ArrayBuffer[ScriptElement]
        val create = new ArrayBuffer[ScriptElement]
        val createTables = new ArrayBuffer[ScriptElement]
        val createSequences = new ArrayBuffer[ScriptElement]
        val addFks = new ArrayBuffer[ScriptElement]
        
        for ((d, newDecl) <- diff) {
            (d, newDecl) match {
                case (d @ ChangeTableDiff(name, renameTo, entriesDiff), newTable: TableModel) =>
                    require(renameTo.isEmpty || renameTo.get == newTable.name)
                    if (renameTo.isDefined)
                        rename += RenameTableStatement(name, renameTo.get)
                    
                    val (drFk, dr, ch, cr, crFk) = d.splitForOrder
                    dropFks ++= alterScript(drFk, newTable)
                    drop ++= alterScript(dr, newTable)
                    change ++= alterScript(ch, newTable)
                    create ++= alterScript(cr, newTable)
                    addFks ++= alterScript(crFk, newTable)
                case (DropTableDiff(table), _) =>
                    dropTables += DropTableStatement(table.name, false)
                    if (!table.foreignKeys.isEmpty)
                        dropFks += AlterTableStatement(table.name, table.foreignKeys.map(fk =>
                                TableDdlStatement.DropForeignKey(fk.name.getOrThrow("cannot get unnamed key"))))
                case (CreateTableDiff(table), _) =>
                    createTables ++= modelSerializer.serializeTable(table.dropForeignKeys)
                    if (!table.foreignKeys.isEmpty)
                        addFks += AlterTableStatement(table.name, table.foreignKeys.map(fk => TableDdlStatement.AddForeignKey(fk)))
                case (CreateSequenceDiff(sequence), _) =>
                    createSequences += CreateSequenceStatement(sequence.name)
                case (DropSequenceDiff(sequence), _) =>
                    dropSequences += DropSequenceStatement(sequence.name)
                    
            }
        }
        // XXX: drop default must be before drop sequence
        Seq[ScriptElement]() ++ rename ++ dropFks ++ dropTables ++ drop ++ dropSequences ++ createSequences ++ change ++ create ++ createTables ++ addFks
    }
    
    def serializeCreateTableDiff(c: CreateTableDiff) =
        modelSerializer.serializeTable(c.table)
    
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
        val newDeclsMap: Map[String, DatabaseDecl] = Map(newModel.decls.map(o => (o.name, o)): _*)
        val oldDeclsMap: Map[String, DatabaseDecl] = Map(oldModel.decls.map(o => (o.name, o)): _*)
        
        def newDecl(d: DatabaseDeclDiff) = d match {
            case d: ChangeTableDiff => newDeclsMap(d.newName)
            case DropTableDiff(t) => t
            case CreateTableDiff(t) => t
            case CreateSequenceDiff(s) => s
            case DropSequenceDiff(s) => s
        }
        
        serializeChangeTableDiffs(diff.declDiff.map(d => (d, newDecl(d))))
        
        // XXX: sequences are lost
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
