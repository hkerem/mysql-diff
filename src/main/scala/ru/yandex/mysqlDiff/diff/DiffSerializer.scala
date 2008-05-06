package ru.yandex.mysqlDiff.diff

import model._
import script._

object TableScriptBuilder {

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
    
    // XXX: unused
    def alterPrimaryKeyScript(pd: PrimaryKeyDiff, table: TableModel) = {
        import AlterTableStatement._
        val ops = pd match {
            case DropPrimaryKeyDiff =>
                List(DropPrimaryKey)
            case CreatePrimaryKeyDiff(pk) =>
                List(AddPrimaryKey(pk))
            case ChangePrimaryKeyDiff(oldPk, newPk) =>
                List(DropPrimaryKey, AddPrimaryKey(newPk))
        }
        AlterTableStatement(table.name, ops)
    }
    
    def alterRegularIndexScript(id: IndexDiff, table: TableModel) = {
        import AlterTableStatement._
        val op = id match {
            case DropIndexDiff(name) => DropIndex(name)
            case CreateIndexDiff(index) => AddIndex(index)
            //case ChangeIndexDiff(_, _) // XXX: model need to be revised
        }
        new AlterTableStatement(table.name, op)
    }
    
    def alterIndexScript(id: IndexDiff, table: TableModel) = id match {
        case pd: PrimaryKeyDiff => alterPrimaryKeyScript(pd, table)
        case _ => alterRegularIndexScript(id, table)
    }
    
    def alterScript(diff: ChangeTableDiff, model: TableModel): Seq[ScriptElement] = {

        List(CommentElement("-- Modify Table \"" + model.name + "\"")) ++
        diff.columnDiff.map(alterColumnScript(_, model)) ++
        diff.indexDiff.map(alterIndexScript(_, model)) ++
        List(CommentElement("-- End modify Table \"" + model.name + "\""))
        
        // XXX: sort: drop, then change, then create
    }
}

object DiffSerializer {
    def serializeToScript(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[ScriptElement] =
    {
        val newTablesMap: Map[String, TableModel] = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: Map[String, TableModel] = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        diff.tableDiff.flatMap(tbl => tbl match {
            case CreateTableDiff(t) => CreateTableStatement(t) :: Nil
            case DropTableDiff(name) => DropTableStatement(name) :: Nil
            case diff @ ChangeTableDiff(name, renameTo, columnDiff, indexDiff) =>
                    renameTo.map(RenameTableStatement(name, _)) ++
                            TableScriptBuilder.alterScript(diff, newTablesMap(diff.newName))
        })
    }
    
    def serialize(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): String = {
        val options = ScriptSerializer.Options.multiline
        ScriptSerializer.serialize(serializeToScript(diff, oldModel, newModel), options)
    }
}

// vim: set ts=4 sw=4 et:
