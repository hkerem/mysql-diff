package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


abstract class SqlObjectType(val name: String) {

}


case class DataType(override val name: String, val length: Option[Int]) 
        extends SqlObjectType(name: String)
{
    override def toString = "" + name + "[" + length.getOrElse("default") + "]" 
}

case class ColumnModel(override val name: String, val dataType: DataType) 
        extends SqlObjectType(name: String)
{
  val isNotNull: boolean = false
  val indexes: Map[String, IndexModel] = new HashMap()
  override def toString: String = {
     val result = "" + name + " " + dataType + " "
     return result
  } 
}

case class ConstraintModel(override val name: String ) 
        extends SqlObjectType(name: String) {
  
}

class IndexModel(override val name: String, val columns: Seq[ColumnModel], isUnique: boolean) 
       extends ConstraintModel(name: String) 
{
  
}

case class ForeighKey(override val name: String, 
       val localColumns: Seq[ColumnModel], 
       val externalTable: TableModel,
       val externalColumns: Seq[ColumnModel])
       extends ConstraintModel(name: String)
{
     
}

abstract class DatabaseDeclaration(override val name: String) 
        extends SqlObjectType(name: String);

case class TableModel(override val name: String, val columns: Seq[ColumnModel]) 
        extends DatabaseDeclaration(name: String) {
  val columnsMap: Map[String, ColumnModel] = new HashMap()
  val primaryKey: List[ColumnModel] = null;
  val constraints: List[ConstraintModel] = null;
}

case class DatabaseModel(val declarations: Seq[DatabaseDeclaration])