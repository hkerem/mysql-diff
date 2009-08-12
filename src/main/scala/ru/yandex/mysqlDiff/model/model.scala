package ru.yandex.mysqlDiff.model

import scala.collection.mutable._

import util.CollectionUtils._

import Implicits._

class TableName(name: String, schema: Option[String], catalog: Option[String])

abstract class Property {
    def propertyType: PropertyType
}
abstract class PropertyType {
    type Value <: Property
}

// XXX: add HasProperties trait
case class PropertyMap[T <: PropertyType, V <: Property](val properties: Seq[V]) {
    //type V = T#Value
    
    def propertyTypes = properties.map(_.propertyType)
    
    // check there are no duplicate property types
    require(propertyTypes.unique.size == properties.length)
    
    protected def copy(properties: Seq[V]): this.type = new PropertyMap[T, V](properties).asInstanceOf[this.type]
    
    def find[U <: T](pt: U): Option[U#Value] =
        properties.find(_.propertyType == pt).map(_.asInstanceOf[U#Value])
    
    def map(f: V => V): this.type =
        copy(properties.map(f))
    
    def withDefaultProperty(property: V): this.type =
        if (find(property.propertyType.asInstanceOf[T]).isDefined) this
        else copy(properties ++ Seq(property))
    
    def withDefaultProperties(ps: Seq[V]): this.type = {
        var r: this.type = this
        for (p <- ps) r = r.withDefaultProperty(p)
        r
    }
    
    def removePropertyByType(pt: T): this.type =
        copy(properties.filter(_.propertyType != pt))
    
    def removeProperty(p: V): this.type =
        copy(properties.filter(_ != p))
    
    def overrideProperty(o: V): this.type =
        copy(removePropertyByType(o.propertyType.asInstanceOf[T]).properties ++ List(o))
    
    def overrideProperties(ps: Seq[V]): this.type = {
        var r: this.type = this
        for (p <- ps) r = r.overrideProperty(p)
        r
    }
    
    def addProperty(p: V): this.type =
        copy(properties ++ List(p))
    
}

abstract class SqlValue extends script.SqlExpr

case object NullValue extends SqlValue

class NumberValue(val value: BigDecimal) extends SqlValue {
    require(value != null)
    
    override def equals(that: Any) =
        if (that.isInstanceOf[NumberValue]) {
            value.compare(that.asInstanceOf[NumberValue].value) == 0
        } else false
    
    override def toString = "NumberValue("+ value.toString +")"
}

object NumberValue {
    def apply(value: BigDecimal): NumberValue = new NumberValue(value)
    def apply(value: Int): NumberValue = apply(BigDecimal(value))
    def apply(value: String): NumberValue =
        try {
            apply(BigDecimal(value))
        } catch {
            case e: java.lang.NumberFormatException =>
                throw new MysqlDiffException("cannot parse '"+ value +"' as number: "+ e, e)
        }
    // XXX: bad idea
    def unapply(nv: NumberValue): Option[Int] = Some(nv.value.intValue)
}

case class StringValue(value: String) extends SqlValue

case class BooleanValue(value: Boolean) extends SqlValue

/** Any date time value 4.4.3.4 */
abstract class TemporalValue extends SqlValue {
    val value: String
}

trait WithTimeZoneValue extends TemporalValue {
    // def timeZone: DateTimeZone
}

case class TimestampValue(override val value: String) extends TemporalValue
case class TimestampWithTimeZoneValue(override val value: String) extends WithTimeZoneValue
case class DateValue(override val value: String) extends TemporalValue
case class TimeValue(override val value: String) extends TemporalValue
case class TimeWithTimeZoneValue(override val value: String) extends WithTimeZoneValue

// used as default value
case object NowValue extends SqlValue

object DataTypeNameKey
object DataTypeLengthKey
object DataTypePrecisionKey
object DataTypeScaleKey
object ElementTypeKey

abstract case class DataType(name: String) {
    require(name.toUpperCase == name, "data type name must be upper-case")
    require(name.matches("[\\w ]+"))
    
    def properties: Seq[(Any, Any)] = Seq(DataTypeNameKey -> name) ++ customProperties
    def customProperties: Seq[(Any, Any)]
}

trait DataTypeWithLength {
    val name: String
    val length: Option[Int]
}

object DataTypeWithLength {
    def unapply(dt: DataTypeWithLength) = Some((dt.name, dt.length))
}

case class DefaultDataType(override val name: String, length: Option[Int])
    extends DataType(name) with DataTypeWithLength
{
    
    def withName(n: String) =
        new DefaultDataType(n, this.length)
    
    def withLength(l: Option[Int]) =
        new DefaultDataType(this.name, l)
    
    override def customProperties = length.map(DataTypeLengthKey -> _).toList
    
    override def toString =
        name + length.map("(" + _ + ")").getOrElse("")
}

case class NumericDataType(precision: Option[Int], scale: Option[Int]) extends DataType("NUMERIC") {
    override val name = "NUMERIC"
    override def customProperties =
        (precision.map(DataTypePrecisionKey -> _) ++ scale.map(DataTypeScaleKey -> _)).toList
}

case class ArrayDataType(elementType: DataType) extends DataType("ARRAY") {
    // XXX: does not work for multidim arrays
    override def customProperties = List(ElementTypeKey -> elementType)
}

abstract class DataTypes {
    def varchar(length: Int): DataType = make("VARCHAR", Some(length))
    
    def int: DataType
    
    final def make(name: String): DataType = 
        make(name, None)
    
    def make(name: String, length: Option[Int]): DataType =
        new DefaultDataType(name.toUpperCase, length)

    def resolveTypeNameAlias(name: String) = name.toUpperCase
    
    def normalize(dt: DataType) = dt match {
        case dt: DefaultDataType => make(resolveTypeNameAlias(dt.name), dt.length)
        case dt => dt
    }
    
    def isAnyChar(name: String) = resolveTypeNameAlias(name).matches(".*CHAR")
    
    def isAnyDateTime(name: String) =
        List("DATE", "TIME", "DATETIME", "TIMESTAMP") contains resolveTypeNameAlias(name)
    
    def isAnyNumber(name: String) =
        resolveTypeNameAlias(name).matches("(|TINY|SMALL|BIG)INT(EGER)?") ||
                (List("NUMBER", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC")
                        contains resolveTypeNameAlias(name))
    
    def isLengthIgnored(name: String) = false
    def isLengthAllowed(name: String) =
        !(isAnyDateTime(name) || resolveTypeNameAlias(name).matches("(TINY|MEDIUM|LONG|)(TEXT|BLOB)"))

    def equivalent(a: DataType, b: DataType) = {
        val (_, _, ab) = compareSeqs(
            normalize(a).properties, normalize(b).properties, (a: (Any, Any), b: (Any, Any)) => a._1 == b._1)
        ab.forall { case (a, b) => a == b }
    }
}

object DataTypesTests extends org.specs.Specification {
    import Environment.defaultContext._
    
    // no tests yet
}

abstract class TableEntry

case class ColumnProperties(ps: Seq[ColumnProperty])
    extends PropertyMap[ColumnPropertyType, ColumnProperty](ps)
{
    override def copy(properties: Seq[ColumnProperty]) = new ColumnProperties(properties).asInstanceOf[this.type]
    
    /** True iff NOT NULL or unknown */
    def isNotNull = find(NullabilityPropertyType).map(!_.nullable).getOrElse(false)
    
    def defaultValue: Option[script.SqlExpr] = find(DefaultValuePropertyType).map(_.value)
    
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
        import vendor.mysql._
        
        val cp = new ColumnProperties(List(Nullability(false), DefaultValue(NumberValue(3))))
        
        cp.removePropertyByType(NullabilityPropertyType).properties mustNot contain(Nullability(false))
        cp.removePropertyByType(NullabilityPropertyType).properties must contain(DefaultValue(NumberValue(3)))
        
        cp.removePropertyByType(MysqlAutoIncrementPropertyType).properties must contain(Nullability(false))
    }
    
    "overrideProperty" in {
        import vendor.mysql._
        
        val cp = new ColumnProperties(List(Nullability(false), DefaultValue(NumberValue(3))))
        
        cp.overrideProperty(Nullability(true)).properties must contain(Nullability(true))
        cp.overrideProperty(Nullability(true)).properties mustNot contain(Nullability(false))
        cp.overrideProperty(Nullability(true)).properties must contain(DefaultValue(NumberValue(3)))
        
        cp.overrideProperty(MysqlAutoIncrement(true)).properties must contain(MysqlAutoIncrement(true))
        cp.overrideProperty(MysqlAutoIncrement(true)).properties must contain(Nullability(false))
        cp.overrideProperty(MysqlAutoIncrement(true)).properties must contain(DefaultValue(NumberValue(3)))
    }
}

case class ColumnModel(val name: String, val dataType: DataType, properties: ColumnProperties)
    extends TableEntry
{
    def this(name: String, dataType: DataType) = this(name, dataType, ColumnProperties.empty)
    
    require(properties.isModelProperties)
    
    def withProperties(ps: ColumnProperties) =
        new ColumnModel(this.name, this.dataType, ps)
    
    def overrideProperties(os: Seq[ColumnProperty]) =
        withProperties(this.properties.overrideProperties(os))
    
    def withDefaultProperties(os: Seq[ColumnProperty]) =
        withProperties(this.properties.withDefaultProperties(os))
    
    def removeProperty(p: ColumnProperty) =
        withProperties(this.properties.removeProperty(p))

    
    def withDataType(dt: DataType) =
        new ColumnModel(this.name, dt, this.properties)
    
    def withName(n: String) =
        new ColumnModel(n, this.dataType, this.properties)
    
    def isNotNull = properties.isNotNull
    def defaultValue = properties.defaultValue
}

/*
abstract class KeyType
case object PrimaryKey extends KeyType
case object Index extends KeyType
case object ForeignKey extends KeyType
*/

/**
 * Everything except column and table option.
 * Stupid name, but I cannot think better.
 */
abstract class TableExtra extends TableEntry

trait UniqueOrIndexModel extends TableExtra {
    def columns: Seq[IndexColumn]
    def columnNames = columns.map(_.name)
}

// length and asc is for MySQL
case class IndexColumn(name: String, asc: Boolean, length: Option[Int])

object IndexColumn {
    def apply(name: String) = new IndexColumn(name, true, None)
}

case class IndexModel(name: Option[String], override val columns: Seq[IndexColumn], explicit: Boolean)
    extends UniqueOrIndexModel
{
    require(columns.length > 0)
    require(columnNames.unique.size == columnNames.length)
    //require(name.isDefined || !explicit)
}

abstract case class ConstraintModel(name: Option[String]) extends TableExtra {
    require(name.isEmpty || name.get.length > 0)
}
case class UniqueKeyModel(override val name: Option[String], override val columns: Seq[IndexColumn])
    extends ConstraintModel(name) with UniqueOrIndexModel

case class PrimaryKeyModel(override val name: Option[String], columns: Seq[IndexColumn])
    extends ConstraintModel(name)
{
    def columnNames = columns.map(_.name)
}

abstract class ImportedKeyDeferrability
object ImportedKeyInitiallyDeferred extends ImportedKeyDeferrability
object ImportedKeyInitiallyImmediate extends ImportedKeyDeferrability
object ImportedKeyNotDeferrable extends ImportedKeyDeferrability

abstract class ImportedKeyRule
object ImportedKeyNoAction extends ImportedKeyRule
object ImportedKeyCascade extends ImportedKeyRule
object ImportedKeySetNull extends ImportedKeyRule
object ImportedKeySetDefault extends ImportedKeyRule

case class ForeignKeyModel(override val name: Option[String],
        localColumns: Seq[IndexColumn],
        externalTable: String,
        externalColumns: Seq[String],
        updateRule: Option[ImportedKeyRule],
        deleteRule: Option[ImportedKeyRule])
    extends ConstraintModel(name)
{
    def localColumnNames = localColumns.map(_.name)
    
    require(localColumns.length == externalColumns.length)
    // XXX: check externalColumns unique
    
    def properties: Seq[(Any, Any)] =
        Seq("localColumns" -> localColumns.toList,
                "externalTable" -> externalTable,
                "externalColumns" -> externalColumns.toList
            ) ++ name.map("name" -> _) ++
            updateRule.map("updateRule" -> _) ++ deleteRule.map("deleteRule" -> _)
}


abstract class TableOption extends Property {
    override def propertyType: TableOptionType
}

abstract class TableOptionType extends PropertyType {
    override type Value <: TableOption
}

// XXX: rename to TableProperties
case class TableOptions(ps: Seq[TableOption])
    extends PropertyMap[TableOptionType, TableOption](ps)
{
    override def copy(options: Seq[TableOption]) = new TableOptions(options).asInstanceOf[this.type]
}

case class TableModel(override val name: String, columns: Seq[ColumnModel], extras: Seq[TableExtra], options: TableOptions)
    extends DatabaseDecl(name: String)
{

    require(name.length > 0, "table name must not be empty")
    require(columns.length > 0, "table " + name + " must have at least one column")
    require(columnNames.unique.size == columns.size,
        "repeating column names in table " + name + " model")
    require(constraintNames.unique.size == constraintNames.size,
        "repeating constraint names in table " + name + " model")
    
    require(indexNames.unique.size == indexNames.size,
        "repeating index names in table " + name + " model")
    require(indexes.map(_.columnNames.toList).unique.size == indexes.size,
        "repeating indexes in table " + name + " model")
    
    require(primaryKeys.length <= 1)
    
    def explicitIndexes =
        indexes.filter(_.explicit)
    
    def explicitExtras = extras.filter {
        case i: IndexModel => i.explicit
        case _ => true
    }
    
    def entries = columns ++ extras
    
    def explicitEntries = columns ++ explicitExtras
    
    private def primaryKeys = extras.flatMap { case p: PrimaryKeyModel => Some(p); case _ => None }
    def primaryKey = primaryKeys.firstOption
    
    def indexes = extras.flatMap { case i: IndexModel => Some(i); case _ => None }
    def indexNames = indexes.flatMap(_.name)
    def findIndex(name: String) = indexes.find(_.name == Some(name))
    def findIndexWithColumns(cols: Seq[String]) = indexes.find(_.columnNames.toList == cols.toList)
    def index(name: String) =
        findIndex(name).getOrThrow("table " + this.name + " has no index " + name)
    
    def uniqueKeys = extras.flatMap { case u: UniqueKeyModel => Some(u); case _ => None }
    def findUniqueKey(name: String) = uniqueKeys.find(_.name == Some(name))
    def uniqueKey(name: String) =
        findUniqueKey(name).getOrThrow("table " + this.name + " has no unique key " + name)
    
    def foreignKeys = extras.flatMap { case f: ForeignKeyModel => Some(f); case _ => None }
    def findForeignKey(name: String) = foreignKeys.find(_.name == Some(name))
    def foreignKey(name: String) =
        findForeignKey(name).getOrThrow("table " + this.name + " has no foreign key " + name)
    
    def findColumn(name: String) = columns.find(_.name == name)
    def column(name: String) = findColumn(name).getOrThrow("table " + this.name + " has no column " + name)
    def columnNames = columns.map(_.name)
    
    def constraints = extras.flatMap { case c: ConstraintModel => Some(c); case _ => None }
    def constraintNames = constraints.flatMap(_.name)
    
    def indexWithColumns(columns: String*) =
        indexes.find(_.columnNames.toList == columns.toList)
            .getOrThrow("table " + this.name + " has no index with columns " + columns.mkString(", "))
    def uniqueKeyWithColumns(columns: String*) =
        uniqueKeys.find(_.columnNames.toList == columns.toList)
            .getOrThrow("table " + this.name + " has no unique keys with columns " + columns.mkString(", "))
    
    
    def addColumn(c: ColumnModel) =
        withColumns(columns ++ Seq(c))
    
    def dropColumn(name: String) = {
        column(name) // check exists
        withColumns(columns.filter(_.name != name))
    }
    
    def alterColumn(name: String, alter: ColumnModel => ColumnModel) = {
        column(name) // check exists
        withColumns(columns.map {
            case c if c.name == name => alter(c)
            case c => c
        })
    }
    
    def addIndex(i: IndexModel) = {
        val cleared = dropImplicitIndexWithColumnsIfExists(i.columnNames)
        if (i.explicit) {
            cleared.addExtra(i)
        } else {
            if (cleared.findIndexWithColumns(i.columnNames).isDefined)
                cleared
            else
                cleared.addExtra(i)
        }
    }
    
    private def addImplicitIndexForColumns(columns: Seq[String]) =
        if (indexes.exists(i => i.columnNames.take(columns.length).toList == columns.toList))
            this
        else
            addIndex(new IndexModel(None, columns.map(c => new IndexColumn(c, true, None)), false))
    
    def addImplicitIndexes = {
        val columnNamess: Seq[Seq[String]] =
            foreignKeys.map(_.localColumnNames) ++
                uniqueKeys.map(_.columnNames) ++ primaryKeys.map(_.columnNames)
        columnNamess.foldLeft(this) { (table, columnNames) =>
            table.addImplicitIndexForColumns(columnNames)
        }
    }
    
    def dropIndex(name: String) = {
        index(name)
        withExtras(extras.filter { case IndexModel(Some(`name`), _, _) => false; case _ => true })
    }
    
    private def dropImplicitIndexWithColumnsIfExists(columns: Seq[String]) =
        withExtras(extras.filter {
            case i @ IndexModel(_, _, false) if i.columnNames.toList == columns.toList => false
            case _ => true
        })
    
    def addUniqueKey(uk: UniqueKeyModel) =
        addExtra(uk)
    
    def dropIndexOrUniqueKey(name: String) = {
        if (findIndex(name).isDefined) {
            dropIndex(name)
        } else if (findUniqueKey(name).isDefined) {
            withExtras(extras.filter { case UniqueKeyModel(Some(`name`), _) => false; case _ => true })
        } else throw new MysqlDiffException("table " + name + " has no index or unique key " + name)
    }
    
    def addForeignKey(fk: ForeignKeyModel) =
        addExtra(fk)
    
    def dropForeignKey(name: String) = {
        foreignKey(name)
        withExtras(extras.filter { case ForeignKeyModel(Some(`name`), _, _, _, _, _) => false; case _ => true })
    }
    
    def dropForeignKeys =
        withExtras(extras.filter { case ForeignKeyModel(_, _, _, _, _, _) => false; case _ => true })
    
    def addPrimaryKey(pk: PrimaryKeyModel) =
        addExtra(pk)
    
    def dropPrimaryKey = {
        primaryKey.get
        withExtras(extras.filter { case p: PrimaryKeyModel => false; case _ => true })
    }
    
    def addExtra(e: TableExtra) =
        withExtras(extras ++ Seq(e))
    
    /** Columns is contained in PK */
    def isPk(name: String) =
        primaryKey.isDefined && primaryKey.get.columnNames.contains(name)
    
    def withName(n: String) =
        new TableModel(n, columns, extras, options)
    
    def withColumns(cs: Seq[ColumnModel]) =
        new TableModel(name, cs, extras, options)
    
    def withExtras(es: Seq[TableExtra]) =
        new TableModel(name, columns, es, options)
    
    def withOptions(os: TableOptions) =
        new TableModel(name, columns, extras, os)
        
    
    def overrideOptions(os: Seq[TableOption]) =
        withOptions(this.options.overrideProperties(os))
    
    def withDefaultOptions(os: Seq[TableOption]) =
        withOptions(this.options.withDefaultProperties(os))
}

// XXX: add type
case class SequenceModel(override val name: String) extends DatabaseDecl(name)

abstract class DatabaseDecl(val name: String) {
}

case class DatabaseModel(decls: Seq[DatabaseDecl])
{
    // no objects with same name
    require(decls.map(_.name).unique.size == decls.length)
    
    def ++(that: DatabaseModel) =
        DatabaseModel(this.decls ++ that.decls)
    
    def tables: Seq[TableModel] =
        decls.flatMap {
            case t: TableModel => Some(t)
            case _ => None
        }
    
    def singleTable: TableModel = decls match {
        case Seq(t: TableModel) => t
        case _ => throw new MysqlDiffException("expecting single table")
    }
    
    def sequences: Seq[SequenceModel] =
        decls.flatMap {
            case s: SequenceModel => Some(s)
            case _ => None
        }
    
    
    def table(name: String) = findTable(name).getOrElse(throw new MysqlDiffException("DB has no table " + name))
    
    def sequence(name: String) = findSequence(name).get
    
    def findTable(name: String) = tables.find(_.name == name)
    
    def findSequence(name: String) = sequences.find(_.name == name)
    
    def dropTable(name: String) = {
        table(name) // check exists
        dropTableIfExists(name)
    }
    
    def dropSequence(name: String) = {
        sequence(name) // check exists
        dropSequenceIfExists(name)
    }
    
    def dropTableIfExists(name: String) =
        // XXX: check it is a table
        new DatabaseModel(decls.filter(_.name != name))
    
    def dropSequenceIfExists(name: String) =
        // XXX: check it is sequence
        new DatabaseModel(decls.filter(_.name != name))
    
    def createDecl(decl: DatabaseDecl) =
        new DatabaseModel(decls ++ Seq(decl))
    
    def createTable(table: TableModel) =
        createDecl(table)
    
    def createSequence(sequence: SequenceModel) =
        createDecl(sequence)
    
    def alterTable(name: String, alter: TableModel => TableModel) = {
        table(name) // check exists
        new DatabaseModel(decls.map {
            case t: TableModel if t.name == name => alter(t)
            case t => t
        })
    }
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

// XXX: change to nonNullable
case class Nullability(nullable: Boolean) extends ColumnProperty {
    override def propertyType = NullabilityPropertyType
}

case object NullabilityPropertyType extends ColumnPropertyType {
    override type Value = Nullability
}

case class DefaultValue(value: script.SqlExpr) extends ColumnProperty {
    override def propertyType = DefaultValuePropertyType
}

case object DefaultValuePropertyType extends ColumnPropertyType {
    override type Value = DefaultValue
}

case class DataTypeProperty(dataType: DataType) extends ColumnProperty {
    override def propertyType = DataTypePropertyType
}

case object DataTypePropertyType extends ColumnPropertyType {
    override type Value = DataTypeProperty
}

object ModelTests extends org.specs.Specification {
    include(ColumnPropertiesTests)
    include(DataTypesTests)

    import Environment.defaultContext._
    
    "model with repeating column names are not allowed" in {
        try {
            new TableModel("users", List(new ColumnModel("id", dataTypes.int), new ColumnModel("id", dataTypes.varchar(9))), Nil, Nil)
            fail("two id columns should not be allowed")
        } catch {
            case e: IllegalArgumentException =>
        }
    }
    
    "keys with repeating column names must not be allowed" in {
        try {
            new IndexModel(Some("ii"), List(IndexColumn("a"), IndexColumn("b"), IndexColumn("a")), true)
            fail("two columns with same name should not be allowed in key")
        } catch {
            case e: IllegalArgumentException =>
        }
    }
}

// vim: set ts=4 sw=4 et:
