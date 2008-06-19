package ru.yandex.mysqlDiff.jdbc

import java.sql._

import model._

import scalax.control.ManagedResource

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

class JdbcModelExtractorException(msg: String, cause: Throwable) extends Exception(msg, cause)

/*
 * TBD:
 * Extract table engine, default charset
 * Extract keys
 */
object JdbcModelExtractor {
    import JdbcUtils._
    
    import vendor.mysql._
    
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
    
    abstract class SchemaExtractor(conn: Connection) {
        lazy val currentSchema = {
            val schema = conn.getMetaData.getURL.replaceFirst("\\?.*", "").replaceFirst(".*/", "")
            require(schema.length > 0)
            schema
        }
        
        def extractTable(tableName: String): TableModel = {
            val data = conn.getMetaData
            val columns = data.getColumns(null, currentSchema, tableName, "%")
            
            val defaultValuesFromMysql = getMysqlColumnDefaultValues(tableName)
            
            val columnsList = read(columns) { columns =>
                val colName = columns.getString("COLUMN_NAME")
                
                val colType = columns.getString("TYPE_NAME")
                
                def getIntOption(rs: ResultSet, columnName: String) = {
                    val v = rs.getInt(columnName)
                    if (rs.wasNull) None
                    else Some(v)
                }
                
                val colTypeSize = getIntOption(columns, "COLUMN_SIZE")

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
                
                val dataType = DataType(colType, colTypeSize)

                val defaultValueFromDb =
                    // http://bugs.mysql.com/36699
                    if (true) defaultValuesFromMysql.find(_._1 == colName).get._2
                    else columns.getString("COLUMN_DEF")
                
                val defaultValue = parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))

                val isUnsigned = false
                val isZerofill = false
                val characterSet: Option[String] = None
                val collate: Option[String] = None

                val props = new ColumnProperties(List[ColumnProperty]() ++ nullable ++ defaultValue ++ autoIncrement)
                new ColumnModel(colName, dataType, props)
            }
            
            val pk = extractPrimaryKey(tableName, conn)
            
            def columnExistsInPk(name: String) =
                pk.exists(_.columns.exists(_ == name))
            
            // MySQL adds PK to indexes, so exclude
            val indexes = extractIndexes(tableName, conn)
                    .filter(pk.isEmpty || _.columns.toList != pk.get.columns.toList)
            
            new TableModel(tableName, columnsList.toList, pk, indexes, getTableOptions(tableName))
        }
        
        def extractPrimaryKey(tableName: String, conn: Connection): Option[PrimaryKey] = {
            val rs = conn.getMetaData.getPrimaryKeys(null, currentSchema, tableName)
            
            case class R(pkName: String, columnName: String, keySeq: int)
            
            val r0 = read(rs) { rs =>
                R(rs.getString("PK_NAME"), rs.getString("COLUMN_NAME"), rs.getInt("KEY_SEQ"))
            }
            
            val r = stableSort(r0, (r: R) => r.keySeq)
            
            if (r.isEmpty) None
            else {
                val pkName = r.first.pkName
                
                // check all rows have the same name
                for (R(p, _, _) <- r)
                    if (p != pkName)
                        throw new IllegalStateException("got different names for pk: " + p + ", " + pkName)
                
                // MySQL names primary key PRIMARY
                val pkNameO = if (pkName != null && pkName != "PRIMARY") Some(pkName) else None
                
                Some(new PrimaryKey(pkNameO, r.map(_.columnName)))
            }
        }
        
        // regular indexes
        def extractIndexes(tableName: String, conn: Connection): Seq[IndexModel] = {
            val rs = conn.getMetaData.getIndexInfo(null, null, tableName, false, false)
            
            case class R(indexName: String, nonUnique: Boolean, ordinalPosition: Int,
                    columnName: String, ascOrDesc: String)
            {
                def unique = !nonUnique
            }
            
            val r = read(rs) { rs =>
                R(rs.getString("INDEX_NAME"), rs.getBoolean("NON_UNIQUE"), rs.getInt("ORDINAL_POSITION"),
                        rs.getString("COLUMN_NAME"), rs.getString("ASC_OR_DESC"))
            }
            
            val indexNames = Set(r.map(_.indexName): _*).toSeq
            
            indexNames.map { indexName =>
                val rowsWithName = r.filter(_.indexName == indexName)
                val rows = stableSort(rowsWithName, (r: R) => r.ordinalPosition)
                
                val unique = rows.first.unique
                
                IndexModel(Some(indexName), rows.map(_.columnName), unique)
            }
        }

        protected def findTableNames() = {
            val data = conn.getMetaData

            val rs = data.getTables(null, currentSchema, "%", List("TABLE").toArray)

            read(rs) { rs =>
                rs.getString("TABLE_NAME")
            }
        }
        
        def mapTableOptions(rs: ResultSet) =
            (rs.getString("TABLE_NAME"), List(TableOption("ENGINE", rs.getString("ENGINE"))))
        
        protected def findTablesOptions(): Seq[(String, Seq[TableOption])] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, currentSchema)
            val rs = ps.executeQuery()
            read(rs)(mapTableOptions _)
        }
        
        protected def findTableOptions(tableName: String): Seq[TableOption] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, currentSchema)
            ps.setString(2, tableName)
            val rs = ps.executeQuery()
            rs.next()
            mapTableOptions(rs)._2
        }
        
        def getTableOptions(tableName: String): Seq[TableOption]
        
        def findMysqlTablesColumnDefaultValues(): Seq[(String, Seq[(String, String)])] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, currentSchema)
            val rs = ps.executeQuery()
            
            val triples = read(rs) { rs =>
                val tableName = rs.getString("table_name")
                val columnName = rs.getString("column_name")
                val defaultValue = rs.getString("column_default")
                (tableName, columnName, defaultValue)
            }
            
            val tables = new scala.collection.mutable.HashMap[String, ArrayBuffer[(String, String)]]
            for ((tableName, columnName, defaultValue) <- triples) {
                tables.getOrElseUpdate(tableName, new ArrayBuffer[(String, String)]) += ((columnName, defaultValue))
            }
            
            tables.toList
        }
        
        def findMysqlColumnDefaultValues(tableName: String) = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, currentSchema)
            ps.setString(2, tableName)
            val rs = ps.executeQuery()
            
            read(rs) { rs =>
                val columnName = rs.getString("column_name")
                val defaultValue = rs.getString("column_default")
                (columnName, defaultValue)
            }
        }
        
        def getMysqlColumnDefaultValues(tableName: String): Seq[(String, String)]
    }
    
    class SingleTableSchemaExtractor(conn: Connection) extends SchemaExtractor(conn) {
        override def getTableOptions(tableName: String) = findTableOptions(tableName)
        
        override def getMysqlColumnDefaultValues(tableName: String) = findMysqlColumnDefaultValues(tableName)
    }
    
    class AllTablesSchemaExtractor(conn: Connection) extends SchemaExtractor(conn) {
    
        def extract(): DatabaseModel =
            new DatabaseModel("xx", extractTables())
        
        private val cachedTableNames = new Lazy(findTableNames())
        def tableNames = cachedTableNames.get
        
        private val cachedTablesOptions = new Lazy(findTablesOptions())
        
        def getTableOptions(tableName: String): Seq[TableOption] =
            cachedTablesOptions.get.find(_._1 == tableName).get._2
        
        val cachedMysqlColumnDefaultValues = new Lazy(findMysqlTablesColumnDefaultValues())
        
        def getMysqlColumnDefaultValues(tableName: String) =
            cachedMysqlColumnDefaultValues.get.find(_._1 == tableName).get._2
    
        def extractTables(): Seq[TableModel] = {
            tableNames.map(extractTable _)
        }
    
    }
    
    protected def parseDefaultValueFromDb(s: String, dataType: DataType): Option[SqlValue] = {
        if (s == null) None
        else if (dataType.isAnyChar) {
            if (s matches "'.*'") Some(StringValue(s.replaceFirst("^'", "").replaceFirst("'$", "")))
            else Some(StringValue(s))
        }
        else if (s == "NULL") None
        else if (dataType.isAnyDateTime) {
            if (s == "CURRENT_TIMESTAMP") Some(NowValue)
            else Some(StringValue(s))
        }
        else if (dataType.isAnyNumber)
            Some(script.parser.SqlParserCombinator.parseValue(s))
        else Some(StringValue(s))
    }
    
    
    private def read[T](rs: ResultSet)(f: ResultSet => T) = {
        var r = List[T]()
        while (rs.next()) {
            r += f(rs)
        }
        r
    }
    
    def extractTables(conn: ManagedResource[Connection]): Seq[TableModel] =
        for (c <- conn) yield new AllTablesSchemaExtractor(c).extractTables()
    
    def extract(conn: ManagedResource[Connection]): DatabaseModel =
        for (c <- conn) yield new AllTablesSchemaExtractor(c).extract()
    
    def search(url: String): Seq[TableModel] = {
        extractTables(connection(url))
    }


    def parse(jdbcUrl: String): DatabaseModel = new DatabaseModel("database", search(jdbcUrl))
    
    def parseTable(tableName: String, jdbcUrl: String) =
        for (c <- connection(jdbcUrl)) yield new SingleTableSchemaExtractor(c).extractTable(tableName)
    
    def main(args: scala.Array[String]) {
    	def usage() {
    	    Console.err.println("usage: JdbcModelExtractor jdbc-url [table-name]")
    	}
    	
    	val model = args match {
    	    case Seq(jdbcUrl) =>
                parse(jdbcUrl)
    	    case Seq(jdbcUrl, tableName) =>
                new DatabaseModel("xx", List(parseTable(tableName, jdbcUrl)))
            case _ =>
                usage(); exit(1)
    	}
    	
    	print(ModelSerializer.serializeDatabaseToText(model))
    }
}

import scalax.testing._
object JdbcModelExtractorTests extends org.specs.Specification {
    Class.forName("com.mysql.jdbc.Driver")
        
    val testDsUrl = "jdbc:mysql://fastshot:3306/mysql_diff_test"
    val testDsUser = "test"
    val testDsPassword = "test"
    
    val conn = ManagedResource(DriverManager.getConnection(testDsUrl, testDsUser, testDsPassword))
    
    private def execute(q: String) {
        for (c <- conn) {
            c.createStatement().execute(q)
        }
    }
    
    private def dropTable(tableName: String) {
        execute("DROP TABLE IF EXISTS " + tableName)
    }
    
    def extractTable(name: String) =
        for (c <- conn) yield new JdbcModelExtractor.SingleTableSchemaExtractor(c).extractTable(name)
    
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
    
    "table options" in {
        dropTable("dogs")
        execute("CREATE TABLE dogs (id INT) ENGINE=InnoDB")
        val table = extractTable("dogs")
        table.options must contain(TableOption("ENGINE", "InnoDB"))
    }
    
    "PK is not in indexes list" in {
        dropTable("files")
        execute("CREATE TABLE files (id INT, PRIMARY KEY(id))")
        
        val table = extractTable("files")
        table.indexes.length must_== 0
        table.primaryKey.get.columns.toList must_== List("id")
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
        table.column("a").defaultValue must_== None
        table.column("b").defaultValue must_== Some(StringValue(""))
        table.column("c").defaultValue must_== Some(StringValue("x"))
        table.column("d").defaultValue must_== None
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
    
}

// vim: set ts=4 sw=4 et:
