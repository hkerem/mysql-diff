package ru.yandex.mysqlDiff.model

import scala.collection.mutable._

class DataType(val name: String, val length: Option[Int]) {
    override def toString = "" + name + "[" + length.getOrElse("default") + "]" 
}

class ColumnModel(val name: String, val dataType: DataType) {
  val isNotNull: boolean = false
  val indexes: Map[String, IndexModel] = new HashMap()
}

class ConstraintModel(val name: String ) {
  
}

class IndexModel(override val name: String, val columns: Seq[ColumnModel], isUnique: boolean) 
       extends ConstraintModel(name: String) 
{
  
}

class ForeighKey(override val name: String, 
       val localColumns: Seq[ColumnModel], 
       val externalTable: TableModel,
       val externalColumns: Seq[ColumnModel])
       extends ConstraintModel(name: String)
{
     
}

abstract class DatabaseDeclaration

class TableModel(val name: String, val columns: Seq[ColumnModel]) extends DatabaseDeclaration {
  val columnsMap: Map[String, ColumnModel] = new HashMap()
  val primaryKey: List[ColumnModel] = null;
  val constraints: List[ConstraintModel] = null;
}

class DatabaseModel(val declarations: Seq[DatabaseDeclaration])