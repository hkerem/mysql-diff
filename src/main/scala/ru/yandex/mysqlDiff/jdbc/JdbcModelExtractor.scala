package ru.yandex.mysqlDiff.jdbc

import java.sql._

import scalax.control.ManagedResource

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

import model._
import util._

// XXX: drop it
import vendor.mysql._

import Implicits._

class JdbcModelExtractorException(msg: String, cause: Throwable) extends Exception(msg, cause)

/*
 * TBD:
 * Extract table engine, default charset
 * Extract keys
 */
class JdbcModelExtractor(context: Context) {
    import JdbcUtils._
    
    import context._
    
    import vendor.mysql._

    import MetaDao._
    import MysqlMetaDao._

    // http://bugs.mysql.com/36699
    private val PROPER_COLUMN_DEF_MIN_MYSQL_VERSION = MysqlServerVersion.parse("5.0.51")
    
    class Lazy[T](create: => T) {
        var value: Option[T] = None
        def get = {
            if (value.isEmpty) value = Some(create)
            value.get
        }
        def isCreated = value.isDefined
    }
    
    protected def createMetaDao(jt: JdbcTemplate) = new MetaDao(jt)
    
    /**
     * Stateful object.
     */
    protected abstract class SchemaExtractor(val jt: JdbcTemplate) {
        import jt._
        
        protected val dao = createMetaDao(jt)
        
        lazy val currentDb = {
            val db = metaData(_.getURL.replaceFirst("\\?.*", "").replaceFirst(".*/", ""))
            require(db.length > 0)
            db
        }
        
        def currentCatalog: String = null
        def currentSchema: String = currentDb
        
        protected def parseTableColumn(columns: ResultSet) = {
            val colName = columns.getString("COLUMN_NAME")
            
            val colType = columns.getString("TYPE_NAME")
            
            def getIntOption(rs: ResultSet, columnName: String) = {
                val v = rs.getInt(columnName)
                if (rs.wasNull) None
                else Some(v)
            }
            
            val colTypeSize = getIntOption(columns, "COLUMN_SIZE")
                .filter { x => dataTypes.isLengthAllowed(colType) }

            val nullable = columns.getString("IS_NULLABLE") match {
                case "YES" => Some(Nullability(true))
                case "NO" => Some(Nullability(false))
                case "" => None
            }
            
            val autoIncrement = columns.getString("IS_AUTOINCREMENT") match {
                case "YES" => Some(AutoIncrement(true))
                case "NO" => Some(AutoIncrement(false))
                case "" => None
            }
            
            val defaultValueFromDb =
                columns.getString("COLUMN_DEF")
            
            val dataType = dataTypes.make(colType, colTypeSize,
                    Seq[DataTypeOption]())
            
            val defaultValue = parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))
            
            val props = Seq[ColumnProperty]() ++ nullable ++ defaultValue ++ autoIncrement
            new ColumnModel(colName, dataType, props)
        }
        
        def extractTableColumns(tableName: String): Seq[ColumnModel] = metaData { data =>
            val columns = data.getColumns(currentCatalog, currentSchema, tableName, "%")
            read(columns)(parseTableColumn _)
        }
        
        def extractTable(tableName: String): TableModel = {
            val columnsList = extractTableColumns(tableName)
            
            val pk = getPrimaryKey(tableName)
            
            def columnExistsInPk(name: String) =
                pk.exists(_.columns.exists(_ == name))
            
            val fks = getFks(tableName)
            
            // MySQL adds PK to indexes, so exclude
            val indexes = getIndexes(tableName)
                    .filter(pk.isEmpty || _.columns.toList != pk.get.columns.toList)
                    .filter(i => !(fks.map(_.localColumns.toList) contains i.columns.toList))
            
            new TableModel(tableName, columnsList.toList, pk, indexes ++ fks, getTableOptions(tableName))
        }
        
        def getPrimaryKey(tableName: String): Option[PrimaryKeyModel] =
            dao.findPrimaryKey(currentCatalog, currentSchema, tableName)
        
        def getIndexes(tableName: String): Seq[IndexModel] =
            dao.findIndexes(currentCatalog, currentSchema, tableName)

        def getFks(tableName: String): Seq[ForeignKeyModel] =
            dao.findImportedKeys(currentCatalog, currentSchema, tableName)
        
        /** Not including PK */
        def getKeys(tableName: String): Seq[KeyModel] =
            getIndexes(tableName) ++ getFks(tableName)

        def getTableOptions(tableName: String): Seq[TableOption]
        
    }
    
    protected class SingleTableSchemaExtractor(jt: JdbcTemplate) extends SchemaExtractor(jt) {
        override def getTableOptions(tableName: String) =
            dao.findTableOptions(currentCatalog, currentSchema, tableName)
        
    }
    
    protected class AllTablesSchemaExtractor(jt: JdbcTemplate) extends SchemaExtractor(jt) {
    
        def extract(): DatabaseModel =
            new DatabaseModel(extractTables())
        
        private val cachedTableNames = new Lazy(dao.findTableNames(currentCatalog, currentSchema))
        def tableNames = cachedTableNames.get
        
        private val cachedTablesOptions = new Lazy(dao.findTablesOptions(currentCatalog, currentSchema))
        
        def getTableOptions(tableName: String): Seq[TableOption] =
            cachedTablesOptions.get.find(_._1 == tableName).get._2
        
        def extractTables(): Seq[TableModel] = {
            tableNames.map(extractTable _)
        }
    
    }
    
    protected def parseDefaultValueFromDb(s: String, dataType: DataType): Option[SqlValue] = {
        if (s == null) Some(NullValue) // None
        else if (dataTypes.isAnyChar(dataType.name)) {
            if (s matches "'.*'") Some(StringValue(s.replaceFirst("^'", "").replaceFirst("'$", "")))
            else Some(StringValue(s))
        }
        else if (s == "NULL") None
        else if (dataTypes.isAnyDateTime(dataType.name)) {
            if (s == "CURRENT_TIMESTAMP") Some(NowValue)
            else Some(StringValue(s))
        }
        else if (dataTypes.isAnyNumber(dataType.name)) {
            Some(sqlParserCombinator.parseValue(s))
        }
        else Some(StringValue(s))
    }
    
    
    protected def newAllTablesSchemaExtractor(jt: JdbcTemplate) =
        new AllTablesSchemaExtractor(jt)
    
    protected def newSingleTableSchemaExtractor(jt: JdbcTemplate) =
        new SingleTableSchemaExtractor(jt)
    
    def extractTables(ds: LiteDataSource): Seq[TableModel] =
        newAllTablesSchemaExtractor(ds).extractTables()
    
    def extractTable(tableName: String, ds: LiteDataSource): TableModel =
        newSingleTableSchemaExtractor(ds).extractTable(tableName)
    
    def extract(ds: LiteDataSource): DatabaseModel =
        newAllTablesSchemaExtractor(ds).extract()
    
    def search(url: String): Seq[TableModel] = {
        extractTables(LiteDataSource.driverManager(url))
    }


    def parse(jdbcUrl: String): DatabaseModel = new DatabaseModel(search(jdbcUrl))
    
    def parseTable(tableName: String, jdbcUrl: String) =
        newSingleTableSchemaExtractor(LiteDataSource.driverManager(jdbcUrl)).extractTable(tableName)
    
    def main(args: scala.Array[String]) {
        def usage() {
            Console.err.println("usage: JdbcModelExtractor jdbc-url [table-name]")
        }
        
        val model = args match {
            case Seq(jdbcUrl) =>
                parse(jdbcUrl)
            case Seq(jdbcUrl, tableName) =>
                new DatabaseModel(List(parseTable(tableName, jdbcUrl)))
            case _ =>
                usage(); exit(1)
        }
        
        print(ModelSerializer.serializeDatabaseToText(model))
    }
}

object JdbcModelExtractorTests extends org.specs.Specification {
    import vendor.mysql.MysqlTestDataSourceParameters._
    
    import MysqlContext._
    
    private def execute(q: String) {
        jdbcTemplate.execute(q)
    }
    
    private def dropTable(tableName: String) {
        execute("DROP TABLE IF EXISTS " + tableName)
    }
    
    def extractTable(name: String) =
        jdbcModelExtractor.extractTable(name, ds)
    
    "Simple Table" in {
        dropTable("bananas")
        execute("CREATE TABLE bananas (id INT, color VARCHAR(100), PRIMARY KEY(id))")
        
        val table = extractTable("bananas")
        
        assert("bananas" == table.name)
        
        assert("id" == table.columns(0).name)
        assert("INT" == table.columns(0).dataType.name)
        
        assert("color" == table.columns(1).name)
        assert("VARCHAR" == table.columns(1).dataType.name)
        assert(100 == table.columns(1).dataType.length.get)
        
        assert(List("id") == table.primaryKey.get.columns.toList)
    }
    
    "Indexes" in {
        dropTable("users")
        execute("CREATE TABLE users (first_name VARCHAR(20), last_name VARCHAR(20), age INT, INDEX age_k(age), UNIQUE KEY(first_name, last_name), KEY(age, last_name))")
        
        val table = extractTable("users")
        
        val ageK = table.indexes.find(_.name.get == "age_k").get
        List("age") must_== ageK.columns.toList
        ageK.isUnique must_== false
        
        val firstLastK = table.indexWithColumns("first_name", "last_name")
        firstLastK.isUnique must_== true
        
        val ageLastK = table.indexWithColumns("age", "last_name")
        ageLastK.isUnique must_== false
    }
    
    "PK is not in indexes list" in {
        dropTable("files")
        execute("CREATE TABLE files (id INT, PRIMARY KEY(id))")
        
        val table = extractTable("files")
        table.indexes.length must_== 0
        table.primaryKey.get.columns.toList must_== List("id")
    }
    
    "Foreign keys" in {
        dropTable("citizen")
        dropTable("city")
        dropTable("person")
        
        execute("CREATE TABLE city (id INT PRIMARY KEY, name VARCHAR(10)) ENGINE=InnoDB")
        execute("CREATE TABLE person(id1 INT, id2 INT, PRIMARY KEY(id1, id2)) ENGINE=InnoDB")
        // http://community.livejournal.com/levin_matveev/20802.html
        execute("CREATE TABLE citizen (id INT PRIMARY KEY, city_id INT, pid1 INT, pid2 INT, " +
                "FOREIGN KEY (city_id) REFERENCES city(id), " +
                "FOREIGN KEY (pid1, pid2) REFERENCES person(id1, id2)" +
                ") ENGINE=InnoDB")
        
        val citizen = extractTable("citizen")
        val city = extractTable("city")
        val person = extractTable("person")
        
        citizen.fks must haveSize(2)
        
        val fkc = citizen.fks.find(_.localColumns.toList == List("city_id")).get
        fkc.localColumns must beLike { case Seq("city_id") => true }
        fkc.externalColumns must beLike { case Seq("id") => true }
        fkc.externalTableName must_== "city"
        
        val fkp = citizen.fks.find(_.localColumns.toList == List("pid1", "pid2")).get
        fkp.localColumns must beLike { case Seq("pid1", "pid2") => true }
        fkp.externalColumns must beLike { case Seq("id1", "id2") => true }
        fkp.externalTableName must_== "person"
        
        // no sure
        //citizen.indexes must haveSize(0)
        
        city.fks must haveSize(0)
        person.fks must haveSize(0)
    }
    
    "fetch table option ENGINE" in {
        dropTable("dogs")
        execute("CREATE TABLE dogs (id INT) ENGINE=InnoDB")
        val table = extractTable("dogs")
        table.options.properties must contain(MysqlEngineTableOption("InnoDB"))
    }
    
    "fetch table option COLLATE" in {
        dropTable("cats")
        execute("CREATE TABLE cats (id INT) COLLATE=cp1251_bin")
        val table = extractTable("cats")
        table.options.properties must contain(MysqlCollateTableOption("cp1251_bin"))
    }
    
    "DEFAULT NOW()" in {
        dropTable("cars")
        execute("CREATE TABLE cars (id INT, created TIMESTAMP DEFAULT NOW())")
        
        val table = extractTable("cars")
        val created = table.column("created")
        created.defaultValue must_== Some(NowValue)
    }
    
    "MySQL string DEFAULT values" in {
        dropTable("jets")
        execute("CREATE TABLE jets (a VARCHAR(2), b VARCHAR(2) DEFAULT '', c VARCHAR(2) DEFAULT 'x', " +
            "d VARCHAR(2) NOT NULL, e VARCHAR(2) NOT NULL DEFAULT '', f VARCHAR(2) NOT NULL DEFAULT 'y')")
        
        val table = extractTable("jets")
        //table.column("a").defaultValue must_== None
        table.column("b").defaultValue must_== Some(StringValue(""))
        table.column("c").defaultValue must_== Some(StringValue("x"))
        //table.column("d").defaultValue must_== None
        table.column("e").defaultValue must_== Some(StringValue(""))
        table.column("f").defaultValue must_== Some(StringValue("y"))
    }
    
    "unspecified AUTO_INCREMENT" in {
        dropTable("ships")
        execute("CREATE TABLE ships (id INT NOT NULL, name VARCHAR(10), PRIMARY KEY(id))")
        
        val t = extractTable("ships")
        t.column("id").properties.autoIncrement must_== Some(false)
        //t.column("name").properties.autoIncrement must_== None
    }
    
    "fetch column CHARACTER SET and COLLATE" in {
        dropTable("qwqw")
        execute("CREATE TABLE qwqw (a VARCHAR(2), b VARCHAR(2) CHARACTER SET utf8 COLLATE utf8_bin)")
        
        val table = extractTable("qwqw")
        val a = table.column("a")
        val b = table.column("b")
        
        b.dataType.options.properties must contain(MysqlCharacterSet("utf8"))
        b.dataType.options.properties must contain(MysqlCollate("utf8_bin"))
    }
    
    "DATETIME without length" in {
        for (t <- List("DATETIME", "TEXT")) {
            val table = t + "_without_length_test"
            dropTable(table)
            execute("CREATE TABLE " + table + "(a " + t + ")")
            val tp = extractTable(table).column("a").dataType
            (tp.name, tp.length) must_== (t, None)
        }
    }
}

// vim: set ts=4 sw=4 et:
