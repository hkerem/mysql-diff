package ru.yandex.mysqlDiff.diff

import model._
import script._

object TableScriptBuilder {
    import script.{AlterTableStatement => ats}

    def alterColumnScript(cd: ColumnDiff, table: TableModel) = {
        import AlterTableStatement._
        val op = cd match {
            case CreateColumnDiff(c) => AddColumn(c)
            case DropColumnDiff(name) => DropColumn(name)
            case ChangeColumnDiff(name, Some(newName), diff) =>
                    ChangeColumn(name, table.column(newName))
            case ChangeColumnDiff(name, None, diff) =>
                    ModifyColumn(table.column(name))
        }
        AlterTableStatement(table.name, List(op))
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
    
    def alterKeyScript(d: KeyDiff, table: TableModel) =
        AlterTableStatement(table.name, alterKeyStmts(d))
    
    def alterScript(diff: ChangeTableDiff, model: TableModel): Seq[ScriptElement] = {

        List[ScriptElement]() ++
        List(CommentElement("-- " + diff.toString)) ++
        diff.columnDiff.map(alterColumnScript(_, model)) ++
        diff.keyDiff.map(alterKeyScript(_, model)) ++
        List[ScriptElement]()
        
        // XXX: sort: drop, then change, then create
    }
}

object DiffSerializer {
    // XXX: rename to serialize
    def serializeToScript(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[ScriptElement] =
    {
        val newTablesMap: Map[String, TableModel] = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: Map[String, TableModel] = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        diff.tableDiff.flatMap(tbl => tbl match {
            case CreateTableDiff(t) => ModelSerializer.serializeTable(t) :: Nil
            case DropTableDiff(name) => DropTableStatement(name) :: Nil
            case diff @ ChangeTableDiff(name, renameTo, columnDiff, indexDiff) =>
                    renameTo.map(RenameTableStatement(name, _)) ++
                            TableScriptBuilder.alterScript(diff, newTablesMap(diff.newName))
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

// vim: set ts=4 sw=4 et:
