package ru.yandex.mysqlDiff
package model

import scala.collection.mutable._

import scala.Seq

import util.CollectionUtils._

import Implicits._

class TableName(name: String, schema: Option[String], catalog: Option[String])

/** Base class for strongly typed property */
abstract class Property {
    def propertyType: PropertyType
}
abstract class PropertyType {
    type Value <: Property
}

/** Stronly typed properties */
// XXX: add HasProperties trait
class PropertyMap[T <: PropertyType, V <: Property](val properties: Seq[V]) {
    //type V = T#Value
    
    /* Get types of properties in this map */
    def propertyTypes = properties.map(_.propertyType)
    
    // check there are no duplicate property types
    require(propertyTypes.unique.size == properties.length)
    
    protected def copy(properties: Seq[V]): this.type =
        new PropertyMap[T, V](properties).asInstanceOf[this.type]
    
    /** Find a property value by name */
    def find[U <: T](pt: U): Option[U#Value] =
        properties.find(_.propertyType == pt).map(_.asInstanceOf[U#Value])
    
    /** Change properties by specified function */
    def map(f: V => V): this.type =
        copy(properties.map(f))
    
    /** Add property, if no property of this type exists */
    def withDefaultProperty(property: V): this.type =
        if (find(property.propertyType.asInstanceOf[T]).isDefined) this
        else copy(properties ++ Seq(property))
    
    /** Repeat {@link #withDefaultProperty} for each property in the list */
    def withDefaultProperties(ps: Seq[V]): this.type = {
        var r: this.type = this
        for (p <- ps) r = r.withDefaultProperty(p)
        r
    }
    
    /** Remove property with given type from this map */
    def removePropertyByType(pt: T): this.type =
        copy(properties.filter(_.propertyType != pt))
    
    /** Remove property from this map iff it type and value equal to given */
    def removeProperty(p: V): this.type =
        copy(properties.filter(_ != p))
    
    /** Add property to this map, remove old property of that type if it is in this map */
    def overrideProperty(o: V): this.type =
        copy(removePropertyByType(o.propertyType.asInstanceOf[T]).properties ++ List(o))
    
    /** Repeat {@link #overrideProperty} for each property in the list */
    def overrideProperties(ps: Seq[V]): this.type = {
        var r: this.type = this
        for (p <- ps) r = r.overrideProperty(p)
        r
    }
    
    /**
     * Add property to this map.
     * This method fails if there is already property of this type in the map.
     */
    def addProperty(p: V): this.type =
        copy(properties ++ List(p))
    
}

abstract class SqlValue extends script.SqlExpr

/** Representation of SQL constant */
case object NullValue extends SqlValue

/** SQL NUMBER. Represented as BigDecimal, and this should be changed */
class NumberValue(val value: BigDecimal) extends SqlValue {
    require(value != null)
    
    override def equals(that: Any) =
        if (that.isInstanceOf[NumberValue]) {
            value.compare(that.asInstanceOf[NumberValue].value) == 0
        } else false
    
    override def toString = "NumberValue("+ value.toString +")"
}

/** NumberValue utilities */
object NumberValue {
    /** Construct */
    def apply(value: BigDecimal): NumberValue = new NumberValue(value)
    /** Construct */
    def apply(value: Int): NumberValue = apply(BigDecimal(value))
    /** Parse string to NumberValue */
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

/** SQL VARCHAR */
case class StringValue(value: String) extends SqlValue

/** SQL BOOLEAN */
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

/**
 * Representation of <code>NOW()</code> function.
 * Used to store default value.
 * XXX: should extends SqlExpr
 */
case object NowValue extends SqlValue

object DataTypeNameKey
object DataTypeLengthKey
object DataTypePrecisionKey
object DataTypeScaleKey
object ElementTypeKey

/** Base class for data types */
abstract class DataType(val name: String) {
    require(name.toUpperCase == name, "data type name must be upper-case")
    require(name.matches("[\\w ]+"))
    
    def properties: Seq[(Any, Any)] = Seq(DataTypeNameKey -> name) ++ customProperties
    /**
     * Representation of this type as sequence of properties.
     * Used for type comparison.
     */
    def customProperties: Seq[(Any, Any)]
}

/** Some types like VARCHAR have length */
trait DataTypeWithLength {
    val name: String
    val length: Option[Int]
}

object DataTypeWithLength {
    def unapply(dt: DataTypeWithLength) = Some((dt.name, dt.length))
}

/** Universal data type representation */
case class DefaultDataType(override val name: String, length: Option[Int])
    extends DataType(name) with DataTypeWithLength
{
    
    override def customProperties = length.map(DataTypeLengthKey -> _).toList
    
    override def toString =
        name + length.map("(" + _ + ")").getOrElse("")
}

/** Universal numeric data type representation */
case class NumericDataType(precision: Option[Int], scale: Option[Int]) extends DataType("NUMERIC") {
    override def customProperties =
        (precision.map(DataTypePrecisionKey -> _) ++ scale.map(DataTypeScaleKey -> _)).toList
}

/** Universal array representation */
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

object DataTypesTests extends MySpecification {
    import Environment.defaultContext._
    
    // no tests yet
}

abstract class TableEntry

/** Column properties */
case class ColumnProperties(ps: Seq[ColumnProperty])
    extends PropertyMap[ColumnPropertyType, ColumnProperty](ps)
{
    override def copy(properties: Seq[ColumnProperty]) = new ColumnProperties(properties).asInstanceOf[this.type]
    
    /** True iff NOT NULL or unknown */
    def isNotNull = find(NullabilityPropertyType).map(!_.nullable).getOrElse(false)
    
    /** Get the default value */
    def defaultValue: Option[script.SqlExpr] = find(DefaultValuePropertyType).map(_.value)
    
    /** True iff all properties are model properties */
    def isModelProperties = properties.forall(_.isModelProperty)
}

object ColumnProperties {
    val empty = new ColumnProperties(Seq())
}

object ColumnPropertiesTests extends MySpecification {
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

/** Table column model */
case class ColumnModel(
    val name: String,
    val dataType: DataType,
    properties: ColumnProperties = ColumnProperties.empty
)
    extends TableEntry
{
    require(properties.isModelProperties)
    
    def withProperties(ps: ColumnProperties) =
        new ColumnModel(this.name, this.dataType, ps)
    
    def overrideProperties(os: Seq[ColumnProperty]) =
        withProperties(this.properties.overrideProperties(os))
    
    def withDefaultProperties(os: Seq[ColumnProperty]) =
        withProperties(this.properties.withDefaultProperties(os))
    
    def removeProperty(p: ColumnProperty) =
        withProperties(this.properties.removeProperty(p))

    /** Shortcut */
    def isNotNull = properties.isNotNull
    /** Shortcut */
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

/**
 * Representation of a column within index.
 * Length and asc are MySQL extensions.
 */
case class IndexColumn(name: String, asc: Boolean = true, length: Option[Int] = None)

/** Representation of index */
case class IndexModel(name: Option[String], override val columns: Seq[IndexColumn], explicit: Boolean)
    extends UniqueOrIndexModel
{
    // index cannot be empty
    require(columns.length > 0)
    // no column is allowed to be exist twice in the index
    require(columnNames.unique.size == columnNames.length)
}

/** Base for table constraints */
abstract class ConstraintModel(val name: Option[String]) extends TableExtra {
    require(name.isEmpty || name.get.length > 0)
}

/** UNIQUE */
case class UniqueKeyModel(override val name: Option[String], override val columns: Seq[IndexColumn])
    extends ConstraintModel(name) with UniqueOrIndexModel

/** PRIMARY KEY */
case class PrimaryKeyModel(override val name: Option[String], columns: Seq[IndexColumn])
    extends ConstraintModel(name)
{
    /** Shortcut */
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

/** FOREIGN KEY */
case class ForeignKeyModel(override val name: Option[String],
        localColumns: Seq[IndexColumn],
        externalTable: String,
        externalColumns: Seq[String],
        updateRule: Option[ImportedKeyRule],
        deleteRule: Option[ImportedKeyRule])
    extends ConstraintModel(name)
{
    /** Shortcut */
    def localColumnNames = localColumns.map(_.name)
    
    // consistency check
    require(localColumns.length == externalColumns.length)
    // XXX: check externalColumns unique
    
    /** Properties are for comparison */
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

/**
 * TABLE. Central thing in the model.
 */
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
    for ((columnNames, indexes) <- indexes.groupBy(_.columnNames.toList) if indexes.length > 1) {
        throw new Exception("repeating indexes in table " + name + " model: " + columnNames)
    }
    
    require(primaryKeys.length <= 1)
    
    /** Shortcut */
    def explicitIndexes =
        indexes.filter(_.explicit)
    
    def explicitExtras = extras.filter {
        case i: IndexModel => i.explicit
        case _ => true
    }
    
    def entries = columns ++ extras
    
    def explicitEntries = columns ++ explicitExtras
    
    // helper method
    private def primaryKeys = extras.flatMap { case p: PrimaryKeyModel => Some(p); case _ => None }
    /** Shortcut */
    def primaryKey = primaryKeys.headOption // should be .singleOption
    
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
    
    // alter operations
    
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
    
    def withColumns(cs: Seq[ColumnModel]) =
        new TableModel(name, cs, extras, options)
    
    def withExtras(es: Seq[TableExtra]) =
        new TableModel(name, columns, es, options)
    
    def withOptions(os: TableOptions) =
        new TableModel(name, columns, extras, os)
        
    def withName(n: String) =
        new TableModel(n, columns, extras, options)
    
    def overrideOptions(os: Seq[TableOption]) =
        withOptions(this.options.overrideProperties(os))
    
    def withDefaultOptions(os: Seq[TableOption]) =
        withOptions(this.options.withDefaultProperties(os))
}

// XXX: add type
case class SequenceModel(override val name: String) extends DatabaseDecl(name)

/**
 * Top level database object. All such objects must have names.
 */
abstract class DatabaseDecl(val name: String) {
}

/** Database */
case class DatabaseModel(decls: Seq[DatabaseDecl])
{
    // no objects with same name
    require(decls.map(_.name).unique.size == decls.length)
    
    /** Concatenate models */
    def ++(that: DatabaseModel) =
        DatabaseModel(this.decls ++ that.decls)
    
    /** Get tables */
    def tables: Seq[TableModel] =
        decls.flatMap {
            case t: TableModel => Some(t)
            case _ => None
        }
    
    /**
     * Get single table from this database.
     * Throw if database contains more then one table or anything except tables.
     */
    def singleTable: TableModel = decls match {
        case Seq(t: TableModel) => t
        case _ => throw new MysqlDiffException("expecting single table")
    }
    
    /** Get sequences */
    def sequences: Seq[SequenceModel] =
        decls.flatMap {
            case s: SequenceModel => Some(s)
            case _ => None
        }
    
    
    /** Get table by name */
    def table(name: String) = findTable(name).getOrElse(throw new MysqlDiffException("DB has no table " + name))
    
    /** Get sequence by name */
    def sequence(name: String) = findSequence(name).get
    
    /** Find table by name, return <code>None</code> if not found */
    def findTable(name: String) = tables.find(_.name == name)
    
    /** Find sequence by name, return <code>None</code> if not found */
    def findSequence(name: String) = sequences.find(_.name == name)
    
    /**
     * Remove table by name from this database.
     * Table must exist.
     */
    def dropTable(name: String) = {
        table(name) // check exists
        dropTableIfExists(name)
    }
    
    /**
     * Remove sequence by name from this database.
     * Table must exist.
     */
    def dropSequence(name: String) = {
        sequence(name) // check exists
        dropSequenceIfExists(name)
    }

    /** Filter tables by given function. Keep other objects intact */
    def filterTables(tableP: TableModel => Boolean) =
        new DatabaseModel(decls.filter {
            case decl: TableModel => tableP(decl)
            case _ => true
        })

    /** Drop table by name if exists */
    def dropTableIfExists(name: String) = filterTables(_.name != name)

    /** Drop sequence by name if exists */
    def dropSequenceIfExists(name: String) =
        // XXX: check it is sequence
        new DatabaseModel(decls.filter(_.name != name))
    
    /** Add declaration to the database */
    def createDecl(decl: DatabaseDecl) =
        new DatabaseModel(decls ++ Seq(decl))
    
    /** Add table to the database */
    def createTable(table: TableModel) =
        createDecl(table)
    
    /** Add sequence to the database */
    def createSequence(sequence: SequenceModel) =
        createDecl(sequence)
    
    /** Change table by name with given function. Table must exist */
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

object ModelTests extends MySpecification {
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
