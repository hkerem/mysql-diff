package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


class SqlObjectType(val name: String)  {
  def toCreateStatement: String = ""
  var comment: String = null
}

case class DataType(override val name: String, val length: Option[Int]) 
        extends SqlObjectType(name: String)
{
  var parent: ColumnModel = null
  
  var isUnsigned: boolean = false
  var isZerofill: boolean = false
  var characterSet: String = null
  var collate: String = null
  
  override def toString = "" + name + "[" + length.getOrElse(-1) + "] is unsigned: " + isUnsigned + " is zerofill:" + isZerofill
  
    
  override def toCreateStatement = {
    var str = "";
    if (length.isDefined) str = "(" + length.get + ")"
    "" + name + str
  };
}

case class ColumnModel(override val name: String, val dataType: DataType) 
        extends SqlObjectType(name: String)
{
  var parent: TableModel = null
  var isNotNull: Boolean = false
  var isAutoIncrement: Boolean = false
  
  var primaryKey: Boolean = false
  
  override def toString: String = {
    var nullDef = "";
    if (isNotNull) nullDef = " NOT NULL"

    "" + name + " " + dataType + " " + nullDef
  }
  
  override def toCreateStatement = {
    var nullDef = "";
    if (isNotNull) nullDef = " NOT NULL"
    var autoIncrementDef = "";
    if (isAutoIncrement) autoIncrementDef = " AUTO_INCREMENT"
    "" + name + " " + dataType.toCreateStatement + nullDef + autoIncrementDef
  }
}

case class ConstraintModel(override val name: String, val columns: Seq[String]) 
        extends SqlObjectType(name: String) 
{
    override def toCreateStatement: String = ""
    var parent: TableModel = null
}

case class IndexModel(override val name: String, override val columns: Seq[String], isUnique: boolean) 
       extends ConstraintModel(name, columns) 
{
  override def toCreateStatement: String = ""
}

case class PrimaryKeyModel(override val name: String, override val columns: Seq[String])
        extends IndexModel(name, columns, true)

case class ForeighKey(override val name: String, 
       val localColumns: Seq[String], 
       val externalTable: TableModel,
       val externalColumns: Seq[String])
       extends ConstraintModel(name: String, localColumns)
{
  override def toCreateStatement: String = ""
}

case class TableModel(override val name: String, val columns: Seq[ColumnModel]) 
        extends DatabaseDeclaration(name: String) {
  var columnsMap: Map[String, ColumnModel] = new HashMap()
  var primaryKey: PrimaryKeyModel = null;
  var constraints: List[ConstraintModel] = null;
  var keys: List[IndexModel] = null
  
  override def toCreateStatement: String = {
    
    var result = "CREATE TABLE " + name + " (";
    columns.foreach(x => result = result + ",\n" + x.toCreateStatement)
    
    if (primaryKey != null && primaryKey.columns != null && primaryKey.columns.size > 0) {
      result = result + ",\nPRIMARY KEY ("
      for (x <- primaryKey.columns) result  = result + ", " + x
      result = result + ")"
    }
 
    if (constraints != null && constraints.size > 0) {
      for (x <- constraints) {
        val xCreate = x.toCreateStatement
        if (!xCreate.trim().equals("")) result = result + ",\n" + xCreate;        
      }
    }
    (result + ");").replaceAll("\\([\\s\\n]*,[\\s\\n]*", "(")
  }
}

abstract class DatabaseDeclaration(override val name: String) 
extends SqlObjectType(name: String);

case class DatabaseModel(override val name: String, val declarations: Seq[TableModel])
        extends DatabaseDeclaration(name);