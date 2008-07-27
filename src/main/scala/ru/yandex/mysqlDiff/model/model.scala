package ru.yandex.mysqlDiff.model

import scala.collection.mutable._


abstract class SqlValue

case object NullValue extends SqlValue

case class NumberValue(value: Int) extends SqlValue

case class StringValue(value: String) extends SqlValue

// used as default value
case object NowValue extends SqlValue

case class DataType(val name: String, val length: Option[Int]) {
    require(name.toUpperCase == name)
    
    def isAnyChar = name.toUpperCase.matches(".*CHAR")
    def isAnyDateTime = List("DATE", "TIME", "TIMESTAMP") contains name.toUpperCase
    def isAnyNumber = List("NUMBER", "INT", "TINYINT", "BIGINT") contains name.toUpperCase
}

case class MysqlDataType(override val name: String, override val length: Option[Int],
        val isUnsigned: Boolean, val isZerofill: Boolean,
        val characterSet: Option[String], val collate: Option[String])
    extends DataType(name, length)
{
    // assert not all properties have default values (in this case DataType class must be used)
}

object DataType {
    def varchar(length: Int) = new DataType("VARCHAR", Some(length))
    
    def int = apply("INT")
    
    def apply(name: String): DataType = apply(name, None)
}

abstract class TableEntry

case class ColumnProperties(val properties: Seq[ColumnProperty]) {
    def propertyTypes = properties.map(_.propertyType)
    
    // check there are no duplicate property types
    require(Set(propertyTypes: _*).size == properties.length)
    
    def isEmpty = properties.isEmpty
    
    def find[T <: ColumnPropertyType](pt: T): Option[T#ValueType] =
        properties.find(_.propertyType == pt).map(_.asInstanceOf[T#ValueType])
    
    /** True iff NOT NULL or unknown */
    def isNotNull = find(NullabilityPropertyType).map(!_.nullable).getOrElse(false)
    
    def comment: Option[String] = find(CommentPropertyType).map(_.comment)
    
    /** False iff true or unknown */
    def isAutoIncrement = find(AutoIncrementPropertyType).map(_.autoIncrement).getOrElse(false)
    
    def defaultValue: Option[SqlValue] = find(DefaultValuePropertyType).map(_.value)
    
    def autoIncrement: Option[Boolean] = find(AutoIncrementPropertyType).map(_.autoIncrement)
    
    /** True iff all properties are model properties */
    def isModelProperties = properties.forall(_.isModelProperty)
    
    def withDefaultProperty(property: ColumnProperty) =
        if (find(property.propertyType).isDefined) this
        else new ColumnProperties(properties ++ List(property))
    
    def removeProperty(pt: ColumnPropertyType) =
        new ColumnProperties(properties.filter(_.propertyType != pt))
    
    def overrideProperty(o: ColumnProperty) =
        new ColumnProperties(removeProperty(o.propertyType).properties ++ List(o))
    
    def addProperty(p: ColumnProperty) =
        new ColumnProperties(properties ++ List(p))
}

object ColumnProperties {
    val empty = new ColumnProperties(Nil)
}

object ColumnPropertiesTests extends org.specs.Specification {
    "isNotNull" in {
        new ColumnProperties(List(Nullability(true))).isNotNull must_== false
        new ColumnProperties(List(Nullability(false))).isNotNull must_== true
        new ColumnProperties(List(DefaultValue(NumberValue(0)))).isNotNull must_== false
    }
    
    "removeProperty" in {
        val cp = new ColumnProperties(List(Nullability(false), DefaultValue(NumberValue(3))))
        
        cp.removeProperty(NullabilityPropertyType).properties mustNot contain(Nullability(false))
        cp.removeProperty(NullabilityPropertyType).properties must contain(DefaultValue(NumberValue(3)))
        
        cp.removeProperty(AutoIncrementPropertyType).properties must contain(Nullability(false))
    }
    
    "overrideProperty" in {
        val cp = new ColumnProperties(List(Nullability(false), DefaultValue(NumberValue(3))))
        
        cp.overrideProperty(Nullability(true)).properties must contain(Nullability(true))
        cp.overrideProperty(Nullability(true)).properties mustNot contain(Nullability(false))
        cp.overrideProperty(Nullability(true)).properties must contain(DefaultValue(NumberValue(3)))
        
        cp.overrideProperty(AutoIncrement(true)).properties must contain(AutoIncrement(true))
        cp.overrideProperty(AutoIncrement(true)).properties must contain(Nullability(false))
        cp.overrideProperty(AutoIncrement(true)).properties must contain(DefaultValue(NumberValue(3)))
    }
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

abstract class KeyModel(val name: Option[String], val columns: Seq[String]) extends TableEntry

case class IndexModel(override val name: Option[String], override val columns: Seq[String], isUnique: Boolean)
    extends KeyModel(name, columns)
{
    require(columns.length > 0)
}

case class PrimaryKey(override val name: Option[String], override val columns: Seq[String])
    extends KeyModel(name, columns)

case class ForeignKeyModel(override val name: Option[String],
        val localColumns: Seq[String],
        val externalTableName: String,
        val externalColumns: Seq[String])
    extends KeyModel(name, localColumns)
{
    require(!localColumns.isEmpty)
    require(localColumns.length == externalColumns.length)
}

case class TableOption(name: String, value: String)

case class TableModel(override val name: String, columns: Seq[ColumnModel],
        primaryKey: Option[PrimaryKey], keys: Seq[KeyModel], options: Seq[TableOption])
    extends DatabaseDeclaration(name: String)
{
    def this(name: String, columns: Seq[ColumnModel], pk: Option[PrimaryKey], keys: Seq[KeyModel]) =
        this(name, columns, pk, keys, Nil)
    
    def this(name: String, columns: Seq[ColumnModel]) =
        this(name, columns, None, Nil)

    require(columns.length > 0)
    
    def column(name: String) = columns.find(_.name == name).get
    
    /** Regular indexes */
    def indexes = keys.flatMap { case i: IndexModel => Some(i); case _ => None }
    def fks = keys.flatMap { case f: ForeignKeyModel => Some(f); case _ => None }
    
    /** PK then regular indexes then foreign keys */
    def allKeys = primaryKey.toList ++ keys
    
    def indexWithColumns(columns: String*) = indexes.find(_.columns.toList == columns.toList).get
    
    def createLikeThis(newName: String) =
        TableModel(newName, columns, primaryKey, keys, options)
}

abstract class DatabaseDeclaration(val name: String) 

case class DatabaseModel(declarations: Seq[TableModel])
{
    def tables: Seq[TableModel] = declarations
    def table(name: String) = tables.find(_.name == name).get
}

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
    include(ColumnPropertiesTests)
}

// vim: set ts=4 sw=4 et:
