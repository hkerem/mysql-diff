package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


class SqlObjectType(val name: String)  {
  def toCreateStatement: String = ""
}

class DataType(override val name: String, val length: Option[Int]) 
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

class ColumnModel(override val name: String, val dataType: DataType) 
        extends SqlObjectType(name: String)
{
  var parent: TableModel = null
  var isNotNull: Boolean = false
  var indexes: Map[String, IndexModel] = new HashMap()
  override def toString: String = {
     val result = "" + name + " " + dataType + " "
     return result
  }
  
  override def toCreateStatement = {
    var nullDef = "";
    if (isNotNull) nullDef = " NOT NULL"
    "" + name + " " + dataType.toCreateStatement + nullDef
  }
}

class ConstraintModel(override val name: String ) 
        extends SqlObjectType(name: String) {
  override def toCreateStatement: String = ""
}

class IndexModel(override val name: String, val columns: Seq[ColumnModel], isUnique: boolean) 
       extends ConstraintModel(name: String) 
{
  override def toCreateStatement: String = ""
}

class ForeighKey(override val name: String, 
       val localColumns: Seq[ColumnModel], 
       val externalTable: TableModel,
       val externalColumns: Seq[ColumnModel])
       extends ConstraintModel(name: String)
{
  override def toCreateStatement: String = ""
}

abstract class DatabaseDeclaration(override val name: String) 
        extends SqlObjectType(name: String);

class TableModel(override val name: String, val columns: Seq[ColumnModel]) 
        extends DatabaseDeclaration(name: String) {
  val columnsMap: Map[String, ColumnModel] = new HashMap()
  val primaryKey: List[ColumnModel] = null;
  val constraints: List[ConstraintModel] = null;
  
  override def toCreateStatement: String = {
    var result = "CREATE TABLE " + name + " (";
    for (x <- columns) result = result + ",\n" + x.toCreateStatement
    
    if (primaryKey != null && primaryKey.size > 0) {
      result = result + ",\nPRIMARY KEY ("
      for (x <- primaryKey) result  = result + ", " + x.name
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

case class DatabaseModel(val declarations: Seq[DatabaseDeclaration])