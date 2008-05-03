package ru.yandex.mysqlDiff.diff

import ru.yandex.mysqlDiff.model._


object DataTypeScriptBuilder {
    def getCreateScript(model: DataType): String = {
        var charset = ""
        if (model.characterSet != "" && model.characterSet != null) charset = " CHARACTER SET " + model.characterSet
        var collate = ""
        if (model.collate != "" && model.collate != null) collate = " COLLATE " + model.collate
        var unsigned = ""
        if (model.isUnsigned) unsigned = " UNSIGNED"
        var zerofill = ""
        if (model.isZerofill) zerofill = " ZEROFILL"
        var size = ""
        if (model.length.isDefined) size = "(" + model.length.get + ")"
        val result = model.name + size + charset + collate + unsigned + zerofill
        result.trim
    }
}

object ColumnScriptBuilder {
    def getCreateScript(model: ColumnModel) : String = {
        var nullable = ""
        if (model.isNotNull) nullable = "NOT NULL "
        var autoincrement = ""
        if (model.isAutoIncrement) autoincrement = "AUTOINCREMENT "
        var comment = ""
        if (model.comment != "" && model.comment != null) comment = "COMMENT " + model.comment + " "
        var default = ""
        if (model.defaultValue != "" && model.defaultValue != null) default = "DEFAULT " + model.defaultValue + " "
        val dataType = DataTypeScriptBuilder.getCreateScript(model.dataType)
        val result = " " + model.name + " " + dataType + " " + nullable + default + autoincrement + comment
        result.trim
    }

    def getAlterScript(diff: AlterColumn, newModel: ColumnModel): String = {
        if (diff.renameTo.isDefined) "CHANGE " + diff.name + getCreateScript(newModel)
            else "MODIFY " + getCreateScript(newModel) 
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
        val columsDefinition  = model.columns.map(t => ColumnScriptBuilder.getCreateScript(t) + ",")
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
        val createColumn: Seq[String] = createColumns.map(t => "ALTER TABLE " + model.name + " ADD COLUMN " + ColumnScriptBuilder.getCreateScript(t.column) + ";")


        val columnMap = Map(model.columns.map(t => (t.name, t)): _*)
        val alterColumn: Seq[String] = alterColumns.map(t => {
            if (t.renameTo.isDefined)
                "ALTER TABLE " + model.name + " CHANGE COLUMN " + t.name + " " + ColumnScriptBuilder.getCreateScript(columnMap(t.renameTo.get)) + ";"
            else
                "ALTER TABLE " + model.name + " MODIFY COLUMN " + ColumnScriptBuilder.getCreateScript(columnMap(t.name)) + ";"
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

/*
object ScriptBuilder1 {

   private def makeDiffForColumn(a: SqlObjectType, b: SqlObjectType): String = {
       val A = a.asInstanceOf[ColumnModel]
       val B = b.asInstanceOf[ColumnModel]

       val aType = A.dataType
       val bType = B.dataType

       var typeLength: String = "";
       if (bType.length.isDefined)
           typeLength = "(" + bType.length.get + ")"
       else
           typeLength = ""

       var notNullDef = ""
       if (B.isNotNull) notNullDef = " NOT NULL ";

       var unsigned = ""
       if (bType.isUnsigned) unsigned = " UNSIGNED ";

       var zerofill = ""
       if (bType.isZerofill) zerofill = " ZEROFILL ";

       var autoIncreament = ""
       if (B.isAutoIncrement) autoIncreament = " AUTO_INCREMENT "


       if (B.name.equals(A.name))
           "ALTER TABLE " + B.parent.name + " MODIFY COLUMN " + B.name + " " + bType.name + typeLength + unsigned + zerofill + notNullDef + autoIncreament  + ";";
       else
           "ALTER TABLE " + A.parent.name + " CHANGE COLUMN " + A.name + " "  + B.name + " " + bType.name + typeLength + unsigned + zerofill + notNullDef + autoIncreament + ";";
   }

   def getString(x: DiffType):String = x match {

       case DatabaseDiff(a, b, diffList) => {
           var result = ""
           for (x <- diffList) {
               result = result + getString(x) + "\n"
           }
           return result
       }

       case NameDiff(a,b) => {
           ""//nothing
       }

       case DataTypeDiff(a, b) => makeDiffForColumn(a, b)
       case NotNullDiff(a, b) => makeDiffForColumn(a, b)

       case ToIsNull(a, b) =>  a match {
           case TableModel(name, columns) => "DROP TABLE " + name + ";"
           case ColumnModel(name, dataType) => "ALTER TABLE " + a.asInstanceOf[ColumnModel].parent.name + " DROP COLUMN " + name + ";"


//        case PrimaryKeyModel(name, cList) => "ALTER TABLE " + a.asInstanceOf[IndexModel].parent.name + " DROP PRIMARY KEY;"
//        case IndexModel(name, cList, isUnique) => "ALTER TABLE " + a.asInstanceOf[IndexModel].parent.name + " DROP INDEX " + name + ";"
//      hack for bug: https://lampsvn.epfl.ch/trac/scala/ticket/816
           case _ => {
               if (a.isInstanceOf[PrimaryKeyModel]) {
                   "ALTER TABLE " + a.asInstanceOf[IndexModel].parent.name + " DROP PRIMARY KEY;"
               } else
                   if (a.isInstanceOf[IndexModel]) {
                       "ALTER TABLE " + a.asInstanceOf[IndexModel].parent.name + " DROP INDEX " + a.name + ";"
                   } else {
                       ""
                   }
           }
       }

       case PrimaryKeyDiff(a, b) => {
           val pk = b.asInstanceOf[PrimaryKeyModel]
           var pkList = ""
           pk.columns.foreach(x => pkList = pkList + ", " + x)
           pkList = pkList.substring(2)
           "ALTER TABLE " + pk.parent.name +" DROP PRIMARY KEY, ADD PRIMARY KEY (" + pkList + ");"
       }

       case  UniqueKeyDiff(a, b) => {
           val idx = b.asInstanceOf[IndexModel]
           var idxList = ""
           idx.columns.foreach(x => idxList = idxList + ", " + x)
           idxList = idxList.substring(2)
           var isUnique = ""
           if (idx.isUnique) isUnique = "UNIQUE "
           "ALTER TABLE " + idx.parent.name +" DROP INDEX " + a.name + ", ADD " + isUnique + "INDEX " + b.name + " (" + idxList + ");"
       }

       case IndexKeyDiff(a, b) => {
           val idx = b.asInstanceOf[IndexModel]
           var idxList = ""
           idx.columns.foreach(x => idxList = idxList + ", " + x)
           idxList = idxList.substring(2)
           var isUnique = ""
           if (idx.isUnique) isUnique = "UNIQUE "
           "ALTER TABLE " + idx.parent.name +" DROP INDEX " + a.name + ", ADD " + isUnique + "INDEX " + b.name + " (" + idxList + ");"
       }

       case FromIsNull(a, b) => b match {
           case TableModel(name, columns) =>  b.asInstanceOf[TableModel].toCreateStatement
           case ColumnModel(name, dataType) => "ALTER TABLE " + b.asInstanceOf[ColumnModel].parent.name + " ADD COLUMN " + b.toCreateStatement + ";"
           case _ => {
               if (b.isInstanceOf[PrimaryKeyModel]) {
                   val pk = b.asInstanceOf[PrimaryKeyModel]
                   var pkList = ""
                   pk.columns.foreach(x => pkList = pkList + ", " + x)
                   pkList = pkList.substring(2)
                   "ALTER TABLE " + b.asInstanceOf[IndexModel].parent.name + " ADD PRIMARY KEY (" + pkList + ")";
               } else
                   if (b.isInstanceOf[IndexModel]) {
                       val index = b.asInstanceOf[IndexModel]
                       var cList = ""
                       index.columns.foreach(x => cList = cList + ", " + x)
                       cList = cList.substring(2)
                       var isUnique: String = ""
                       if (index.isUnique) isUnique = "UNIQUE "
                           "ALTER TABLE " + b.asInstanceOf[IndexModel].parent.name + " ADD " + isUnique +"INDEX " + b.name + " (" + cList +");"
                   } else {
                       ""
                   }
           }
       }

       case TableDiff(a, b, diffList) => {
           var toNullList =  List[ToIsNull[ColumnModel]]()
           var fromNullList = List[FromIsNull[ColumnModel]]()

           var blockedObjects = Set[DiffType]()

           diffList.foreach(x => x match {
               case ToIsNull(a, b) =>  {
                   val sq = toNullList ++ List(x.asInstanceOf[ToIsNull[ColumnModel]])
                   toNullList = sq.toList
               }

               case FromIsNull(a, b) => {
                   val sq = fromNullList ++ List(x.asInstanceOf[FromIsNull[ColumnModel]])
                   fromNullList = sq.toList
               }
               case _ => {}
           })

           var result = ""


           diffList.foreach(x => if (!blockedObjects.contains(x)) {
               x match {
                   case ToIsNull(a, b) => {
                       var forPrint = Set[DiffType]()

                       var alternative = ""
                       fromNullList.foreach(e => {
                           if (e.to.dataType.name.equals(a.asInstanceOf[ColumnModel].dataType.name)) {
                               val diff = new DataTypeDiff(a.asInstanceOf[ColumnModel], e.to)
                               forPrint = forPrint ++ Set[DiffType](e.asInstanceOf[DiffType])
                               alternative  = alternative  + "-- " + ScriptBuilder.getString(diff) + "\n"
                           }
                       })

                       forPrint.foreach(e => result = result + ScriptBuilder.getString(e.asInstanceOf[DiffType]) + "\n")
                       result = result + ScriptBuilder.getString(x) + "\n"

                       if (!alternative.trim.equals(""))
                            result = result + "--alternative actions for column \"" + a.asInstanceOf[ColumnModel].parent.name + "." + a.name + "\" from source\n" + alternative

                       blockedObjects = blockedObjects ++ forPrint ++ List(x)
                   }

                   case _ => {}
               }
           })

           diffList.foreach(x => if (!blockedObjects.contains(x)) result = result + ScriptBuilder.getString(x) + "\n")


           return result
       }
   }
}
*/
