package ru.yandex.mysqlDiff.diff

import model._
import script._

object ColumnScriptBuilder {
    def getAlterScript(diff: AlterColumn, newModel: ColumnModel): String = {
        if (diff.renameTo.isDefined) "CHANGE " + diff.name + ScriptSerializer.serializeColumn(newModel)
            else "MODIFY " + ScriptSerializer.serializeColumn(newModel) 
    }
}



trait ColumnsListBuilder {
    def getColumnsList(columns: Seq[String]): String = {
        var result = ""
        columns.foreach(x => result = result + ", " + x)
        result.substring(2)
    }
}

object PrimaryKeyScriptBuilder extends ColumnsListBuilder {
    def getCreateScript(model: PrimaryKeyModel): String = {
        if (model.columns.size > 0) {
            "PRIMARY KEY (" + getColumnsList(model.columns) + ")"
        } else ""
    }
}


object IndexScriptBuilder extends ColumnsListBuilder {
    def getCreateScript(model: IndexModel): String = {
        if (model.columns.size > 0) {
            val indexDef = "INDEX " + model.name + " (" + getColumnsList(model.columns) + ")"
            if (model.isUnique) "UNIQUE " + indexDef
                else indexDef 
        } else ""
    }
}

object TableScriptBuilder {
    
    def getCreateScript(model: TableModel): Seq[String] = {
        val tableHeader = List("CREATE TABLE " + model.name + " (")
        val columsDefinition  = model.columns.map(t => ScriptSerializer.serializeColumn(t) + ",")
        val primaryKeyDefinition = model.primaryKey.toList.map(t => PrimaryKeyScriptBuilder.getCreateScript(t) + ",")
        val indexDefinitions = model.keys.map(t => IndexScriptBuilder.getCreateScript(t) + ",")

        val dirtyTableBody = columsDefinition ++ primaryKeyDefinition ++ indexDefinitions
        val tableBody = dirtyTableBody.take(Math.max(dirtyTableBody.size - 1, 0)) ++ dirtyTableBody.lastOption.toList.map(t => t.substring(0, Math.max(0, t.length - 1)))
        val tableEnd = List(");")
        tableHeader ++ tableBody ++ tableEnd
    }

    def getCreateScript(diff: CreateTable): Seq[String] = {
        getCreateScript(diff.table)
    }

    def getDropScript(diff: DropTable): Seq[String] = {
        List("DROP TABLE " + diff.name + ";")
    }

    def getAlterScript(oldModel: TableModel, model: TableModel, diff: TableDiffModel): Seq[String] = {
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
            primaryKeyAlter.map(t => "ALTER TABLE " + model.name + " DROP PRIMARY KEY, ADD PRIMARY KEY (" + PrimaryKeyScriptBuilder.getColumnsList(t.columns) + ");") ++
            indexAlter.map(t => "ALTER TABLE " + model.name + " DROP INDEX " + t.name + ", ADD " + IndexScriptBuilder.getCreateScript(t.index))

        val createIndex: Seq[String] =
                primaryKeyCreate.map(t => "ALTER TABLE " + model.name + " ADD PRIMARY KEY (" + PrimaryKeyScriptBuilder.getColumnsList(t.columns) + ");") ++ 
                indexCreate.map(t => "ALTER TABLE " + model.name + " ADD " + IndexScriptBuilder.getCreateScript(t.index))

        List("\n--Modify Table \"" + oldModel.name + "\"") ++
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
        List("--End modify Table \"" + oldModel.name + "\"")
    }
}

object DiffSerializer {
    def getScript(oldModel: DatabaseModel, newModel: DatabaseModel, diff: DatabaseDiff): Seq[String] = {
        val newTablesMap: scala.collection.Map[String, TableModel]  = Map(newModel.declarations.map(o => (o.name, o)): _*)
        val oldTablesMap: scala.collection.Map[String, TableModel]  = Map(oldModel.declarations.map(o => (o.name, o)): _*)
        diff.tableDiff.flatMap(t => t match {
            case a: CreateTable => TableScriptBuilder.getCreateScript(a)
            case a: DropTable => TableScriptBuilder.getDropScript(a)
            case a: TableDiffModel => {
                    if (a.renameTo.isDefined) TableScriptBuilder.getAlterScript(oldTablesMap(a.name), newTablesMap(a.renameTo.get), a)
                        else TableScriptBuilder.getAlterScript(oldTablesMap(a.name), oldTablesMap(a.name), a) 
            }
        })
    }
}

// vim: set ts=4 sw=4 et:
