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

case class ColumnProperties(val properties: Seq[ColumnProperty]) {
    // XXX: add check each property type appears more then once
    
    def isEmpty = properties.isEmpty
    
    def find[T <: ColumnPropertyType](pt: T): Option[T#ValueType] =
        properties.find(_.propertyType == pt).map(_.asInstanceOf[T#ValueType])
    
    /** True iff NOT NULL or unknown */
    def isNotNull = find(NullabilityPropertyType).map(!_.nullable).getOrElse(false)
    
    def comment: Option[String] = find(CommentPropertyType).map(_.comment)
    
    /** False iff true or unknown */
    def isAutoIncrement = find(AutoIncrementPropertyType).map(_.autoIncrement).getOrElse(false)
    
    def defaultValue: Option[SqlValue] = find(DefaultValuePropertyType).map(_.value)
    
    /** True iff all properties are model properties */
    def isModelProperties = properties.forall(_.isModelProperty)
    
    def withDefaultProperty(property: ColumnProperty) =
        if (find(property.propertyType).isDefined) this
        else new ColumnProperties(properties ++ List(property))
}

object ColumnProperties {
    val empty = new ColumnProperties(Nil)
}

case class ColumnModel(val name: String, val dataType: DataType, properties: ColumnProperties)
    extends TableEntry
{
    def this(name: String, dataType: DataType) = this(name, dataType, ColumnProperties.empty)
    
    require(properties.isModelProperties)
    
    def isNotNull = properties.isNotNull
    def isAutoIncrement = properties.isAutoIncrement
    def defaultValue = properties.defaultValue
    def comment = properties.comment
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
    
    def createLikeThis(newName: String) =
        TableModel(newName, columns, primaryKey, indexes, options)
}

abstract class DatabaseDeclaration(val name: String) 

case class DatabaseModel(override val name: String, val declarations: Seq[TableModel])
    extends DatabaseDeclaration(name)

abstract class ColumnProperty {
    def propertyType: ColumnPropertyType
    
    final def isModelProperty = propertyType.isModelProperty
}

abstract class ColumnPropertyType {
    type ValueType <: ColumnProperty
    
    /** True iff property belongs to column model */
    def isModelProperty = true
}

case class Nullability(nullable: Boolean) extends ColumnProperty {
    override def propertyType = NullabilityPropertyType
}

case object NullabilityPropertyType extends ColumnPropertyType {
    override type ValueType = Nullability
}

case class DefaultValue(value: SqlValue) extends ColumnProperty {
    override def propertyType = DefaultValuePropertyType
}

case object DefaultValuePropertyType extends ColumnPropertyType {
    override type ValueType = DefaultValue
}

case class AutoIncrement(autoIncrement: Boolean) extends ColumnProperty {
    override def propertyType = AutoIncrementPropertyType
}

case object AutoIncrementPropertyType extends ColumnPropertyType {
    override type ValueType = AutoIncrement
}

case class CommentProperty(comment: String) extends ColumnProperty {
    override def propertyType = CommentPropertyType
}

case object CommentPropertyType extends ColumnPropertyType {
    override type ValueType = CommentProperty
}

case class DataTypeProperty(dataType: DataType) extends ColumnProperty {
    override def propertyType = DataTypePropertyType
}

case object DataTypePropertyType extends ColumnPropertyType {
    override type ValueType = DataTypeProperty
}

object ModelTests extends org.specs.Specification {
    "ColumnProperties" in {
        new ColumnProperties(List(Nullability(true))).isNotNull must_== false
        new ColumnProperties(List(Nullability(false))).isNotNull must_== true
        new ColumnProperties(List(DefaultValue(NumberValue(0)))).isNotNull must_== false
    }
}

// vim: set ts=4 sw=4 et:
