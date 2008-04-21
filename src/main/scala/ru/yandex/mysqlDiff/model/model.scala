package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


class SqlObjectType(val name: String)  {
  def toCreateStatement: String = ""
}

case class DataType(override val name: String, val length: Option[Int]) 
        extends SqlObjectType(name: String)
{
  var parent: ColumnModel = null
  override def toString = "" + name + "[" + length.getOrElse(-1) + "]"
  
    
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
  var indexes: Map[String, IndexModel] = new HashMap()
  override def toString: String = {
    var nullDef = "";
    if (isNotNull) nullDef = " NOT NULL"

    val result = "" + name + " " + dataType + " " + nullDef
     return result
  }
  
  override def toCreateStatement = {
    var nullDef = "";
    if (isNotNull) nullDef = " NOT NULL"
    "" + name + " " + dataType.toCreateStatement + nullDef
  }
}

case class ConstraintModel(override val name: String ) 
        extends SqlObjectType(name: String) {
  override def toCreateStatement: String = ""
}

case class IndexModel(override val name: String, val columns: Seq[ColumnModel], isUnique: boolean) 
       extends ConstraintModel(name: String) 
{
  override def toCreateStatement: String = ""
}

case class ForeighKey(override val name: String, 
       val localColumns: Seq[ColumnModel], 
       val externalTable: TableModel,
       val externalColumns: Seq[ColumnModel])
       extends ConstraintModel(name: String)
{
  override def toCreateStatement: String = ""
}

case class TableModel(override val name: String, val columns: Seq[ColumnModel]) 
        extends DatabaseDeclaration(name: String) {
  var columnsMap: Map[String, ColumnModel] = new HashMap()
  var primaryKey: List[ColumnModel] = null;
  var constraints: List[ConstraintModel] = null;
  
  override def toCreateStatement: String = {
    var result = "CREATE TABLE " + name + " (";
    for (x <- columns) result = result + ",\n" + x.toCreateStatement
    
    if (primaryKey != null && primaryKey.size > 0) {
      result = result + ",\nPRIMARY KEY ("
      for (x <- primaryKey) result  = result + ", " + x.name
      result = result + ")"
    }
    if (constraints != null && constraints.size > 0) {
      for (x <- constraints) {
        val xCreate = x.toCreateStatement
        if (!xCreate.trim().equals("")) result = result + ",\n" + xCreate;        
      }
    }
    result = result + ");"
    result = result.replaceAll("\\([\\s\\n]*,[\\s\\n]*", "(")
    result
  }
}

abstract class DatabaseDeclaration(override val name: String) 
extends SqlObjectType(name: String);

case class DatabaseModel(override val name: String, val declarations: Seq[TableModel])
        extends DatabaseDeclaration(name);