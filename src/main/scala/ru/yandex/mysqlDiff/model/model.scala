package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


abstract class SqlValue

case object NullValue extends SqlValue

case class NumberValue(value: Int) extends SqlValue

case class StringValue(value: String) extends SqlValue

// used as default value
case object NowValue extends SqlValue

// XXX: reduce number of fields through subclassing
case class DataType(val name: String, val length: Option[Int], val isUnsigned: Boolean, val isZerofill: Boolean, val characterSet: Option[String], val collate: Option[String])

object DataType {
    def varchar(length: Int) = apply("VARCHAR", Some(length))
    def int = apply("INT")
    
    def apply(name: String, length: Option[Int]): DataType = new DataType(name, length, false, false, None, None)
    def apply(name: String): DataType = apply(name, None)
}

abstract class TableEntry

case class ColumnModel(val name: String, val dataType: DataType) extends TableEntry
{
    // XXX: make all this case class parameters
    var isNotNull: Boolean = false
    var isAutoIncrement: Boolean = false
    var defaultValue: Option[String] = None
    var comment: Option[String] = None

    override def equals(otherO: Any): Boolean = {
        if (otherO == null || !otherO.isInstanceOf[ColumnModel])
            false
        else {
            val other = otherO.asInstanceOf[ColumnModel]
            if (!(name == other.name)) false
            else if (!(comment == other.comment)) false
            else if (isNotNull != other.isNotNull) false
            else if (isAutoIncrement != other.isAutoIncrement) false
            else if (!(defaultValue != other.defaultValue)) false
            else true
        }
    }
}

abstract class ConstraintModel(val name: Option[String], val columns: Seq[String]) extends TableEntry

case class IndexModel(override val name: Option[String], override val columns: Seq[String], isUnique: Boolean)
    extends ConstraintModel(name, columns)
{
    require(columns.length > 0)
}

case class PrimaryKey(override val name: Option[String], override val columns: Seq[String])
    extends IndexModel(name, columns, true)

case class ForeighKey(override val name: Option[String],
        val localColumns: Seq[String],
        val externalTableName: String,
        val externalColumns: Seq[String])
    extends ConstraintModel(name, localColumns)
{
    require(localColumns.length == externalColumns.length)
}

case class TableOption(name: String, value: String)

case class TableModel(override val name: String, columns: Seq[ColumnModel],
        primaryKey: Option[PrimaryKey], indexes: Seq[IndexModel], options: Seq[TableOption])
    extends DatabaseDeclaration(name: String)
{
    def this(name: String, columns: Seq[ColumnModel], pk: Option[PrimaryKey], indexes: Seq[IndexModel]) =
        this(name, columns, pk, indexes, Nil)
    
    def this(name: String, columns: Seq[ColumnModel]) =
        this(name, columns, None, Nil)

    require(columns.length > 0)
    
    def column(name: String) = columns.find(_.name == name).get
    
    /** PK then regular indexes */
    def allIndexes = primaryKey.toList ++ indexes
    
    def indexWithColumns(columns: String*) = indexes.find(_.columns.toList == columns.toList).get
}

abstract class DatabaseDeclaration(val name: String) 

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

case object DataTypeValue extends PropertyType {
    override type ValueType = DataType
    override def get(column: ColumnModel) = column.dataType
}

// vim: set ts=4 sw=4 et:
