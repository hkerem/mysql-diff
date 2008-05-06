package ru.yandex.mysqlDiff.diff

import model._
import script._

object TableScriptBuilder {

    def alterColumnScript(cd: ColumnDiff, table: TableModel) = {
        val op = cd match {
            case CreateColumnDiff(c) => AlterTableStatement.AddColumn(c)
            case DropColumnDiff(name) => AlterTableStatement.DropColumn(name)
            case ChangeColumnDiff(name, Some(newName), diff) =>
                    AlterTableStatement.ChangeColumn(name, table.column(newName))
            case ChangeColumnDiff(name, None, diff) =>
                    AlterTableStatement.ModifyColumn(table.column(name))
        }
        AlterTableStatement(table.name, List(op))
    }
    
    // XXX: unused
    def alterPrimaryKeyScript(pd: PrimaryKeyDiff, table: TableModel) = {
        val ops = pd match {
            case DropPrimaryKey =>
                List(AlterTableStatement.DropPrimaryKey)
            case CreatePrimaryKey(pk) =>
                List(AlterTableStatement.CreatePrimaryKey(pk))
            case AlterPrimaryKey(oldPk, newPk) =>
                List(AlterTableStatement.DropPrimaryKey, AlterTableStatement.CreatePrimaryKey(newPk))
        }
        AlterTableStatement(table.name, ops)
    }
    
    def alterScript(diff: ChangeTableDiff, model: TableModel): Seq[ScriptElement] = {
        val primaryKeyDiff = diff.indexDiff.filter(idx => idx.isInstanceOf[PrimaryKeyDiff])

        val primaryKeyDrop = primaryKeyDiff.filter(idx => idx.isInstanceOf[DropPrimaryKey.type]).map(idx => idx.asInstanceOf[DropPrimaryKey.type])
        val primaryKeyCreate = primaryKeyDiff.filter(idx => idx.isInstanceOf[CreatePrimaryKey]).map(idx => idx.asInstanceOf[CreatePrimaryKey])
        val primaryKeyAlter = primaryKeyDiff.filter(idx => idx.isInstanceOf[AlterPrimaryKey]).map(idx => idx.asInstanceOf[AlterPrimaryKey])

        val indexKeyDiff = diff.indexDiff.filter(idx => !idx.isInstanceOf[PrimaryKeyDiff] && idx.isInstanceOf[IndexDiff])
        val indexDrop: Seq[DropIndex] = indexKeyDiff.filter(idx => idx.isInstanceOf[DropIndex]).map(idx => idx.asInstanceOf[DropIndex])
        val indexCreate: Seq[CreateIndex] = indexKeyDiff.filter(idx => idx.isInstanceOf[CreateIndex]).map(idx => idx.asInstanceOf[CreateIndex])
        val indexAlter: Seq[AlterIndex] =  indexKeyDiff.filter(idx => idx.isInstanceOf[AlterIndex]).map(idx => idx.asInstanceOf[AlterIndex])

        val dropIndex: Seq[String] = primaryKeyDrop.map(idx => "ALTER TABLE " + model.name + " DROP PRIMARY KEY") ++ indexDrop.map(idx => "ALTER TABLE " + model.name + " DROP INDEX " + idx.name)

        val alterIndex: Seq[String] =
            primaryKeyAlter.map(pk => "ALTER TABLE " + model.name + " DROP PRIMARY KEY, ADD " + ScriptSerializer.serializePrimaryKey(pk.newPk)) ++
            indexAlter.map(pk => "ALTER TABLE " + model.name + " DROP INDEX " + pk.name + ", ADD " + ScriptSerializer.serializeIndex(pk.index))

        val createIndex: Seq[String] =
                primaryKeyCreate.map(pk => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializePrimaryKey(pk.pk)) ++ 
                indexCreate.map(idx => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializeIndex(idx.index))

        List(CommentElement("-- Modify Table \"" + model.name + "\"")) ++
        List(CommentElement("-- Drop Index")).filter(o => dropIndex.size > 0) ++
        dropIndex.map(Unparsed(_)) ++
        diff.columnDiff.map(alterColumnScript(_, model)) ++
        List(CommentElement("-- Alter Indexes")).filter(o => alterIndex.size > 0) ++
        alterIndex.map(Unparsed(_)) ++
        List(CommentElement("-- Create Indexes")).filter(o =>createIndex.size > 0) ++
        createIndex.map(Unparsed(_)) ++
        List(CommentElement("-- End modify Table \"" + model.name + "\""))
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
