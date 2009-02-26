package ru.yandex.mysqlDiff.model

import scala.collection.mutable._



abstract class Property {
    def propertyType: PropertyType
}
abstract class PropertyType {
    type Value <: Property
}

case class PropertyMap[T <: PropertyType, V <: Property](val properties: Seq[V]) {
    //type V = T#Value
    
    def propertyTypes = properties.map(_.propertyType)
    
    // check there are no duplicate property types
    require(Set(propertyTypes: _*).size == properties.length)
    
    protected def copy(properties: Seq[V]): this.type = new PropertyMap[T, V](properties).asInstanceOf[this.type]
    
    def isEmpty = properties.isEmpty
    
    def find[U <: T](pt: U): Option[U#Value] =
        properties.find(_.propertyType == pt).map(_.asInstanceOf[U#Value])
    
    def withDefaultProperty(property: V): this.type =
        if (find(property.propertyType.asInstanceOf[T]).isDefined) this
        else copy(properties ++ List(property))
    
    def removeProperty(pt: T): this.type =
        copy(properties.filter(_.propertyType != pt))
    
    def overrideProperty(o: V): this.type =
        copy(removeProperty(o.propertyType.asInstanceOf[T]).properties ++ List(o))
    
    def addProperty(p: V): this.type =
        copy(properties ++ List(p))
}


abstract class SqlValue

case object NullValue extends SqlValue

case class NumberValue(value: Int) extends SqlValue

case class StringValue(value: String) extends SqlValue

// used as default value
case object NowValue extends SqlValue

abstract class DataTypeOption extends Property {
    override def propertyType: DataTypeOptionType
}

abstract class DataTypeOptionType extends PropertyType {
    override type Value <: DataTypeOption
}

abstract case class DataType(name: String, length: Option[Int], options: Seq[DataTypeOption]) {
    require(name.toUpperCase == name)

    def isAnyChar: Boolean
    def isAnyDateTime: Boolean
    def isAnyNumber: Boolean
    def isLengthAllowed: Boolean
    /** @deprecated */
    def normalized: DataType

    def equivalent(other: DataType) = {
        val n1 = normalized
        val n2 = other.normalized
        n1.name == n2.name && n1.length == n2.length && n1.options == n2.options
    }

    override def toString = name

    require(length.isEmpty || isLengthAllowed, "length is not allowed")
}

abstract class DataTypes {
    def varchar(length: Int): DataType = make("VARCHAR", Some(length))
    
    def int: DataType
    
    def make(name: String): DataType = 
        make(name, None)
    
    def make(name: String, length: Option[Int]): DataType =
        make(name, length, Nil)

    def make(name: String, length: Option[Int], options: Seq[DataTypeOption]): DataType 
    
    def resolveTypeNameAlias(name: String) = name
    
    def normalize(dt: DataType) = make(resolveTypeNameAlias(dt.name), dt.length, dt.options)

    def equivalent(typeA: DataType, typeB: DataType) = {
        /*
        def e1(a: DataType, b: DataType) =
            if (a == dataTypes.make("TINYINT", Some(1)) && b == dataTypes.make("BIT", None)) true
            else false
        else if (e1(a, b)) true
        else if (e1(b, a)) true
        */
        val a = normalize(typeA)
        val b = normalize(typeB)
        
        if (a == b) true
        else if (a.name != b.name) false
        else if (a.isAnyNumber) true // ignore size change: XXX: should rather know DB defaults
        else if (a.isAnyDateTime) true // probably
        else a.name == b.name && a.length == b.length // ignoring options for a while; should not ignore if options change
    }
}

abstract class TableEntry

/**
 * @deprecated in favor of PropertyMap
 */
case class ColumnProperties(ps: Seq[ColumnProperty])
    extends PropertyMap[ColumnPropertyType, ColumnProperty](ps)
{
    
    /** True iff NOT NULL or unknown */
    def isNotNull = find(NullabilityPropertyType).map(!_.nullable).getOrElse(false)
    
    override def copy(properties: Seq[ColumnProperty]) = new ColumnProperties(properties).asInstanceOf[this.type]
    
    def comment: Option[String] = find(CommentPropertyType).map(_.comment)
    
    /** False iff true or unknown */
    def isAutoIncrement = find(AutoIncrementPropertyType).map(_.autoIncrement).getOrElse(false)
    
    def defaultValue: Option[SqlValue] = find(DefaultValuePropertyType).map(_.value)
    
    def autoIncrement: Option[Boolean] = find(AutoIncrementPropertyType).map(_.autoIncrement)
    
    /** True iff all properties are model properties */
    def isModelProperties = properties.forall(_.isModelProperty)
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

/*
abstract class KeyType
case object PrimaryKey extends KeyType
case object Index extends KeyType
case object ForeignKey extends KeyType
*/

abstract class KeyModel(val name: Option[String], val columns: Seq[String]) extends TableEntry
{
    require(columns.length > 0)
    require(Set(columns: _*).size == columns.length)
}

case class IndexModel(override val name: Option[String], override val columns: Seq[String], isUnique: Boolean)
    extends KeyModel(name, columns)

case class PrimaryKeyModel(override val name: Option[String], override val columns: Seq[String])
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

abstract class TableOptionType {
    type OptionType <: TableOption
}

case class TableModel(override val name: String, columns: Seq[ColumnModel],
        primaryKey: Option[PrimaryKeyModel], keys: Seq[KeyModel], options: Seq[TableOption])
    extends DatabaseDeclaration(name: String)
{
    def this(name: String, columns: Seq[ColumnModel], pk: Option[PrimaryKeyModel], keys: Seq[KeyModel]) =
        this(name, columns, pk, keys, Nil)
    
    def this(name: String, columns: Seq[ColumnModel]) =
        this(name, columns, None, Nil)

    require(columns.length > 0)
    require(Set(columnNames: _*).size == columns.size, "repeating column names in table " + name + " model")
    
    def column(name: String) = columns.find(_.name == name).get
    def columnNames = columns.map(_.name)
    
    /** Regular indexes */
    def indexes = keys.flatMap { case i: IndexModel => Some(i); case _ => None }
    def fks = keys.flatMap { case f: ForeignKeyModel => Some(f); case _ => None }
    
    /** PK then regular indexes then foreign keys */
    def allKeys = primaryKey.toList ++ keys
    
    def indexWithColumns(columns: String*) = indexes.find(_.columns.toList == columns.toList).get
    
    def createLikeThis(newName: String) =
        TableModel(newName, columns, primaryKey, keys, options)
    
    /** Columns is contained in PK */
    def isPk(name: String) =
        primaryKey.isDefined && primaryKey.get.columns.contains(name)
}

abstract class DatabaseDeclaration(val name: String) 
{
    def toScript = ModelSerializer.serializeDatabaseDeclaration(this)
    def toText = toScript.toText
}

case class DatabaseModel(declarations: Seq[TableModel])
{
    def tables: Seq[TableModel] = declarations
    def table(name: String) = tables.find(_.name == name).get
}

abstract class ColumnProperty extends Property {
    def propertyType: ColumnPropertyType
    
    /** @deprecated */
    final def isModelProperty = propertyType.isModelProperty
}

abstract class ColumnPropertyType extends PropertyType {
    override type Value <: ColumnProperty
    
    /** True iff property belongs to column model */
    def isModelProperty = true
}

case class Nullability(nullable: Boolean) extends ColumnProperty {
    override def propertyType = NullabilityPropertyType
}

case object NullabilityPropertyType extends ColumnPropertyType {
    override type Value = Nullability
}

case class DefaultValue(value: SqlValue) extends ColumnProperty {
    override def propertyType = DefaultValuePropertyType
}

case object DefaultValuePropertyType extends ColumnPropertyType {
    override type Value = DefaultValue
}

case class AutoIncrement(autoIncrement: Boolean) extends ColumnProperty {
    override def propertyType = AutoIncrementPropertyType
}

case object AutoIncrementPropertyType extends ColumnPropertyType {
    override type Value = AutoIncrement
}

case class OnUpdateCurrentTimestamp(set: Boolean) extends ColumnProperty {
    override def propertyType = OnUpdateCurrentTimestampPropertyType
}

case object OnUpdateCurrentTimestampPropertyType extends ColumnPropertyType {
    override type Value = OnUpdateCurrentTimestamp
}

case class CommentProperty(comment: String) extends ColumnProperty {
    override def propertyType = CommentPropertyType
}

case object CommentPropertyType extends ColumnPropertyType {
    override type Value = CommentProperty
}

case class DataTypeProperty(dataType: DataType) extends ColumnProperty {
    override def propertyType = DataTypePropertyType
}

case object DataTypePropertyType extends ColumnPropertyType {
    override type Value = DataTypeProperty
}

object ModelTests extends org.specs.Specification {
    include(ColumnPropertiesTests)

    import Environment.defaultContext._
    
    "model with repeating column names are not allowed" in {
        try {
            new TableModel("users", List(new ColumnModel("id", dataTypes.int), new ColumnModel("id", dataTypes.varchar(9))))
            fail("two id columns should not be allowed")
        } catch {
            case e: IllegalArgumentException =>
        }
    }
    
    "keys with repeating column names must not be allowed" in {
        try {
            new IndexModel(None, List("a", "b", "a"), false)
            fail("two columns with same name should not be allowed in key")
        } catch {
            case e: IllegalArgumentException =>
        }
    }
}

// vim: set ts=4 sw=4 et:
