package ru.yandex.mysqlDiff.diff

import model._
import script._

object TableScriptBuilder {
    
    def getAlterScript(diff: AlterTable, model: TableModel): Seq[String] = {
        val createColumns = diff.columnDiff.filter(t => t.isInstanceOf[CreateColumn]).map(t => t.asInstanceOf[CreateColumn])
        val dropColumns = diff.columnDiff.filter(t => t.isInstanceOf[DropColumn]).map(t => t.asInstanceOf[DropColumn])
        val alterColumns = diff.columnDiff.filter(t => t.isInstanceOf[AlterColumn]).map(t => t.asInstanceOf[AlterColumn])

        val createColumnMap = Map(createColumns.map(t => (t.column.name, t)): _*)
        val dropColumnMap = Map(dropColumns.map(t => (t.name, t)): _*)
        val alterColumnMap = Map(alterColumns.map(t => (t.name, t)): _*)

        val primaryKeyDiff = diff.indexDiff.filter(t => t.isInstanceOf[AbstractPrimaryKeyDiff])

        val primaryKeyDrop = primaryKeyDiff.filter(t => t.isInstanceOf[DropPrimaryKey]).map(t => t.asInstanceOf[DropPrimaryKey])
        val primaryKeyCreate = primaryKeyDiff.filter(t => t.isInstanceOf[CreatePrimaryKey]).map(t => t.asInstanceOf[CreatePrimaryKey])
        val primaryKeyAlter = primaryKeyDiff.filter(t => t.isInstanceOf[AlterPrimaryKey]).map(t => t.asInstanceOf[AlterPrimaryKey])

        val indexKeyDiff = diff.indexDiff.filter(t => !t.isInstanceOf[AbstractPrimaryKeyDiff] && t.isInstanceOf[AbstractIndexDiff])
        val indexDrop: Seq[DropIndex] = indexKeyDiff.filter(t => t.isInstanceOf[DropIndex]).map(t => t.asInstanceOf[DropIndex])
        val indexCreate: Seq[CreateIndex] = indexKeyDiff.filter(t => t.isInstanceOf[CreateIndex]).map(t => t.asInstanceOf[CreateIndex])
        val indexAlter: Seq[AlterIndex] =  indexKeyDiff.filter(t => t.isInstanceOf[AlterIndex]).map(t => t.asInstanceOf[AlterIndex])

        val dropIndex: Seq[String] = primaryKeyDrop.map(t => "ALTER TABLE " + model.name + " DROP PRIMARY KEY") ++ indexDrop.map(t => "ALTER TABLE " + model.name + " DROP INDEX " + t.name + ";")
        val dropColumn: Seq[String] = dropColumns.map(t => "ALTER TABLE " + model.name + " DROP COLUMN " + t.name  + " ;")
        val createColumn: Seq[String] = createColumns.map(t => "ALTER TABLE " + model.name + " ADD COLUMN " + ScriptSerializer.serializeColumn(t.column) + ";")


        val columnMap = Map(model.columns.map(t => (t.name, t)): _*)
        val alterColumn: Seq[String] = alterColumns.map(t => {
            if (t.renameTo.isDefined)
                "ALTER TABLE " + model.name + " CHANGE COLUMN " + t.name + " " + ScriptSerializer.serializeColumn(columnMap(t.renameTo.get)) + ";"
            else
                "ALTER TABLE " + model.name + " MODIFY COLUMN " + ScriptSerializer.serializeColumn(columnMap(t.name)) + ";"
        })

        val alterIndex: Seq[String] =
            primaryKeyAlter.map(t => "ALTER TABLE " + model.name + " DROP PRIMARY KEY, ADD " + ScriptSerializer.serializePrimaryKey(t.newPk) + ";") ++
            indexAlter.map(t => "ALTER TABLE " + model.name + " DROP INDEX " + t.name + ", ADD " + ScriptSerializer.serializeIndex(t.index))

        val createIndex: Seq[String] =
                primaryKeyCreate.map(t => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializePrimaryKey(t.pk) + ";") ++ 
                indexCreate.map(t => "ALTER TABLE " + model.name + " ADD " + ScriptSerializer.serializeIndex(t.index))

        List("\n--Modify Table \"" + model.name + "\"") ++
        List("\n--Drop Index").filter(o => dropIndex.size > 0) ++
        dropIndex ++
        List("--Drop Columns").filter(o => dropColumn.size > 0) ++
        dropColumn ++
        List("--Create Columns").filter(o => createColumn.size > 0) ++
        createColumn ++
        List("--Alter Columns").filter(o => alterColumn.size > 0) ++
        alterColumn ++
        List("--Alter Indexes").filter(o => alterIndex.size > 0) ++
        alterIndex ++
        List("--Create Indexes").filter(o =>createIndex.size > 0) ++
        createIndex ++
        List("--End modify Table \"" + model.name + "\"")
    }
}

object DiffSerializer {
    def serializeToScript(diff: DatabaseDiff, oldModel: DatabaseModel, newModel: DatabaseModel)
            : Seq[ScriptElement] =
    {
        val newTablesMap: Map[String, TableModel] = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: Map[String, TableModel] = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        diff.tableDiff.flatMap(t => t match {
            case CreateTable(t) => CreateTableStatement(t) :: Nil
            case DropTable(name) => DropTableStatement(name) :: Nil
            case diff @ AlterTable(name, renameTo, columnDiff, indexDiff) =>
                    renameTo.map(RenameTableStatement(name, _)) ++
                            TableScriptBuilder.getAlterScript(diff, newTablesMap(diff.newName))
                                    .map(Unparsed(_))
        })
    }
    
    def getScript(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): Seq[String] = {
        serializeToScript(diff, oldModel, newModel)
            .map(ScriptSerializer.serialize(_, ScriptSerializer.Options.multiline))
    }
}

// vim: set ts=4 sw=4 et:
