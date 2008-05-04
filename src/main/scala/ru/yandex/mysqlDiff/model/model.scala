package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


class SqlObjectType(val name: String)  {
    var comment: Option[String] = None
}



case class DataType(val name: String, val length: Option[Int]) {
    var isUnsigned: Boolean = false
    var isZerofill: Boolean = false
    var characterSet: Option[String] = None
    var collate: Option[String] = None

    override def equals(other: Any): Boolean = {
        if (other == null || !other.isInstanceOf[DataType]) false else {
            val data = other.asInstanceOf[DataType] 
            if (data == null) false else
            if (name != data.name) false else
            if (isUnsigned != data.isUnsigned) false else
            if (characterSet != data.characterSet) false else
            if (collate != data.collate) false else
            if (length.isDefined != data.length.isDefined || (length.isDefined && length.get != data.length.get)) false else
            true
        }
    }
}

object DataType {
    def varchar(length: Int) = DataType("VARCHAR", Some(length))
    def int() = DataType("INT", None)
}

case class ColumnModel(override val name: String, val dataType: DataType) 
    extends SqlObjectType(name: String)
{
    var isNotNull: Boolean = false
    var isAutoIncrement: Boolean = false
    var defaultValue: Option[String] = None

    var isUnique: Boolean = false
    var isIndex: Boolean = false
    var primaryKey: Boolean = false

    override def equals(otherO: Any): Boolean = {
        if (otherO == null || !otherO.isInstanceOf[ColumnModel]) false else {
            val other = otherO.asInstanceOf[ColumnModel]
            if (!(name == other.name)) false else
            if (!(comment == other.comment)) false else
            if (isNotNull != other.isNotNull) false else
            if (isAutoIncrement != other.isAutoIncrement) false else
            if (!(defaultValue != other.defaultValue)) false else
            if (isUnique != other.isUnique) false else
            if (isIndex != other.isIndex) false else
            if (primaryKey != other.primaryKey) false else
            true
        }
    }
}

case class ConstraintModel(override val name: String, val columns: Seq[String]) 
    extends SqlObjectType(name: String)
{

}

case class IndexModel(override val name: String, override val columns: Seq[String], isUnique: Boolean)
    extends ConstraintModel(name, columns)
{
    require(columns.length > 0)

    override def equals(otherO: Any): Boolean = {
        if (otherO == null || !otherO.isInstanceOf[IndexModel]) false else {
            val other = otherO.asInstanceOf[IndexModel]    
            if (name != other.name) false else
            if (isUnique != other.isUnique) false else {
                if (columns != null && other.columns != null) {
                    val s1 = Set[String](columns: _*)
                    val s2 = Set[String](other.columns: _*)
                    s1.size == s2.size && s1.subsetOf(s2) && s2.subsetOf(s1)
                } else false
            }
        }
    }
}

case class PrimaryKey(override val name: String, override val columns: Seq[String])
    extends IndexModel(name, columns, true)

case class ForeighKey(override val name: String,
    val localColumns: Seq[String],
    val externalTableName: String,
    val externalColumns: Seq[String])
    extends ConstraintModel(name: String, localColumns)
{

}

case class TableModel(override val name: String, val columns: Seq[ColumnModel]) 
    extends DatabaseDeclaration(name: String)
{
    var columnsMap: Map[String, ColumnModel] = new HashMap()
    var primaryKey: Option[PrimaryKey] = None
    var constraints: List[ConstraintModel] = null
    var keys =  List[IndexModel]()
}

abstract class DatabaseDeclaration(override val name: String) 
    extends SqlObjectType(name: String)

case class DatabaseModel(override val name: String, val declarations: Seq[TableModel])
    extends DatabaseDeclaration(name)

abstract class PropertyType {
    type ValueType
    def get(column: ColumnModel): ValueType
}

case object Nullability extends PropertyType {
    override type ValueType = Boolean
    override def get(column: ColumnModel) = !column.isNotNull
}

case object AutoIncrementality extends PropertyType {
    override type ValueType = Boolean
    override def get(column: ColumnModel) = column.isAutoIncrement
}

case object DefaultValue extends PropertyType {
    override type ValueType = Option[String]
    override def get(column: ColumnModel) = column.defaultValue
}

case object CommentValue extends PropertyType {
    override type ValueType = Option[String]
    override def get(column: ColumnModel) = column.comment
}

case object TypeValue extends PropertyType {
    override type ValueType = DataType
    override def get(column: ColumnModel) = column.dataType
}

// vim: set ts=4 sw=4 et:
