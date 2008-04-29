package ru.yandex.mysqlDiff.diff;

import ru.yandex.mysqlDiff.model._

object ScriptBuilder {
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
  
   def getString(x: DiffType[SqlObjectType]):String = x match {
    
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
      
           var blockedObjects = Set[DiffType[SqlObjectType]]()
      
           diffList.foreach(x => x match {
               case ToIsNull(a, b) =>  {
                   val sq = toNullList ++ List(x.asInstanceOf[ToIsNull[ColumnModel]])
                   toNullList = sq.toList
               }
          
               case FromIsNull(a, b) => {
                   val sq = fromNullList ++ List(x.asInstanceOf[FromIsNull[ColumnModel]])
                   fromNullList = List(sq: _*)
               }
               case _ => {}
           })
     
           var result = ""
      
        
           diffList.foreach(x => if (!blockedObjects.contains(x)) {
               x match {
                   case ToIsNull(a, b) => {
                       var forPrint = Set[DiffType[SqlObjectType]]()
            
                       var alternative = ""
                       fromNullList.foreach(e => {
                           if (e.to.dataType.name.equals(a.asInstanceOf[ColumnModel].dataType.name)) {
                               val diff = new DataTypeDiff(a, e.to)
                               forPrint = forPrint ++ Set[DiffType[SqlObjectType]](e.asInstanceOf[DiffType[SqlObjectType]])
                               alternative  = alternative  + "-- " + ScriptBuilder.getString(diff) + "\n"
                           }
                       })

                       forPrint.foreach(e => result = result + ScriptBuilder.getString(e.asInstanceOf[DiffType[SqlObjectType]]) + "\n")
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