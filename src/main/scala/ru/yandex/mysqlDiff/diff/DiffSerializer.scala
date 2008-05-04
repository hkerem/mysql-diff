package ru.yandex.mysqlDiff.diff

import model._
import script._

object TableScriptBuilder {
    
    def getAlterScript(diff: AlterTable, model: TableModel): Seq[ScriptElement] = {
        val createColumns = diff.columnDiff.filter(column => column.isInstanceOf[CreateColumn]).map(column => column.asInstanceOf[CreateColumn])
        val dropColumns = diff.columnDiff.filter(column => column.isInstanceOf[DropColumn]).map(column => column.asInstanceOf[DropColumn])
        val alterColumns = diff.columnDiff.filter(column => column.isInstanceOf[AlterColumn]).map(column => column.asInstanceOf[AlterColumn])

        val createColumnMap = Map(createColumns.map(column => (column.column.name, column)): _*)
        val dropColumnMap = Map(dropColumns.map(column => (column.name, column)): _*)
        val alterColumnMap = Map(alterColumns.map(column => (column.name, column)): _*)

        val primaryKeyDiff = diff.indexDiff.filter(idx => idx.isInstanceOf[AbstractPrimaryKeyDiff])

        val primaryKeyDrop = primaryKeyDiff.filter(idx => idx.isInstanceOf[DropPrimaryKey]).map(idx => idx.asInstanceOf[DropPrimaryKey])
        val primaryKeyCreate = primaryKeyDiff.filter(idx => idx.isInstanceOf[CreatePrimaryKey]).map(idx => idx.asInstanceOf[CreatePrimaryKey])
        val primaryKeyAlter = primaryKeyDiff.filter(idx => idx.isInstanceOf[AlterPrimaryKey]).map(idx => idx.asInstanceOf[AlterPrimaryKey])

        val indexKeyDiff = diff.indexDiff.filter(idx => !idx.isInstanceOf[AbstractPrimaryKeyDiff] && idx.isInstanceOf[AbstractIndexDiff])
        val indexDrop: Seq[DropIndex] = indexKeyDiff.filter(idx => idx.isInstanceOf[DropIndex]).map(idx => idx.asInstanceOf[DropIndex])
        val indexCreate: Seq[CreateIndex] = indexKeyDiff.filter(idx => idx.isInstanceOf[CreateIndex]).map(idx => idx.asInstanceOf[CreateIndex])
        val indexAlter: Seq[AlterIndex] =  indexKeyDiff.filter(idx => idx.isInstanceOf[AlterIndex]).map(idx => idx.asInstanceOf[AlterIndex])

        val dropIndex: Seq[String] = primaryKeyDrop.map(idx => "ALTER TABLE " + model.name + " DROP PRIMARY KEY") ++ indexDrop.map(idx => "ALTER TABLE " + model.name + " DROP INDEX " + idx.name)
        val dropColumn: Seq[String] = dropColumns.map(idx => "ALTER TABLE " + model.name + " DROP COLUMN " + idx.name)
        val createColumn: Seq[String] = createColumns.map(idx => "ALTER TABLE " + model.name + " ADD COLUMN " + ScriptSerializer.serializeColumn(idx.column))


        val columnMap = Map(model.columns.map(col => (col.name, col)): _*)
        val alterColumn: Seq[String] = alterColumns.map(col => {
            if (t.renameTo.isDefined)
                "ALTER TABLE " + model.name + " CHANGE COLUMN " + col.name + " " + ScriptSerializer.serializeColumn(columnMap(col.renameTo.get))
            else
                "ALTER TABLE " + model.name + " MODIFY COLUMN " + ScriptSerializer.serializeColumn(columnMap(col.name))
        })

        val alterIndex: Seq[String] =
            primaryKeyAlter.map(pk => "ALTER TABLE " + model.name + " DROP PRIMARY KEY, ADD " + ScriptSerializer.serializePrimaryKey(pk.newPk)) ++
            indexAlter.map(pk => "ALTER TABLE " + model.name + " DROP INDEX " + t.name + ", ADD " + ScriptSerializer.serializeIndex(pk.index))

        val createIndex: Seq[String] =
                primaryKeyCreate.map(pk => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializePrimaryKey(pk.pk)) ++ 
                indexCreate.map(idx => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializeIndex(idx.index))

        List(CommentElement("-- Modify Table \"" + model.name + "\"")) ++
        List(CommentElement("-- Drop Index")).filter(o => dropIndex.size > 0) ++
        dropIndex.map(Unparsed(_)) ++
        List(CommentElement("-- Drop Columns")).filter(o => dropColumn.size > 0) ++
        dropColumn.map(Unparsed(_)) ++
        List(CommentElement("-- Create Columns")).filter(o => createColumn.size > 0) ++
        createColumn.map(Unparsed(_)) ++
        List(CommentElement("-- Alter Columns")).filter(o => alterColumn.size > 0) ++
        alterColumn.map(Unparsed(_)) ++
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
            case CreateTable(t) => CreateTableStatement(t) :: Nil
            case DropTable(name) => DropTableStatement(name) :: Nil
            case diff @ AlterTable(name, renameTo, columnDiff, indexDiff) =>
                    renameTo.map(RenameTableStatement(name, _)) ++
                            TableScriptBuilder.getAlterScript(diff, newTablesMap(diff.newName))
        })
    }
    
    def serialize(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): String = {
        val options = ScriptSerializer.Options.multiline
        ScriptSerializer.serialize(serializeToScript(diff, oldModel, newModel), options)
    }
}

// vim: set ts=4 sw=4 et:
