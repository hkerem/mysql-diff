package ru.yandex.mysqlDiff.jdbc

import java.sql._

import model._

import scalax.control.ManagedResource

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

/*
 * TBD:
 * Extract table engine, default charset
 * Extract keys
 */
object JdbcModelExtractor {
    import JdbcUtils._
    
    private def currentMysqlSchema(conn: Connection) = {
        val schema = conn.getMetaData.getURL.replaceFirst("\\?.*", "").replaceFirst(".*/", "")
        require(schema.length > 0)
        schema
    }
    
    def extractMysqlColumnDefaultValues(tableName: String, conn: Connection) = {
        val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
        val ps = conn.prepareStatement(q)
        ps.setString(1, currentMysqlSchema(conn))
        ps.setString(2, tableName)
        
        val values = new ArrayBuffer[(String, String)]
        
        val rs = ps.executeQuery()
        while (rs.next()) {
            val columnName = rs.getString("column_name")
            val defaultValue = rs.getString("column_default")
            values += (columnName, defaultValue)
        }
        
        values.toList
    }
    
    def extractTable(tableName: String, conn: Connection): TableModel = {
        val data = conn.getMetaData
        val columns = data.getColumns(null, null, tableName, "%")
        var columnsList = List[ColumnModel]()
        
        val defaultValuesFromMysql = extractMysqlColumnDefaultValues(tableName, conn)
        
        while (columns.next) {
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
            
            val defaultValue = (defaultValueFromDb match {
                case null => None
                case s =>
                    if (!dataType.name.equalsIgnoreCase("VARCHAR") || s.matches("'.*'"))
                        Some(script.parser.SqlParserCombinator.parseValue(s))
                    else
                        Some(StringValue(s))
            }).map(DefaultValue(_))

            val isUnsigned = false
            val isZerofill = false
            val characterSet: Option[String] = None
            val collate: Option[String] = None

            val props = new ColumnProperties(List[ColumnProperty]() ++ nullable ++ defaultValue ++ autoIncrement)

            val cm = new ColumnModel(colName, dataType, props)

            columnsList = (columnsList ++ List(cm)).toList
        }
        
        val pk = extractPrimaryKey(tableName, conn)
        
        // MySQL adds PK to indexes, so exclude
        val indexes = extractIndexes(tableName, conn)
                .filter(pk.isEmpty || _.columns.toList != pk.get.columns.toList)
        
        new TableModel(tableName, columnsList, pk, indexes)
    }
    
    private def read[T](rs: ResultSet)(f: ResultSet => T) = {
        var r = List[T]()
        while (rs.next()) {
            r += f(rs)
        }
        r
    }
    
    def extractPrimaryKey(tableName: String, conn: Connection): Option[PrimaryKey] = {
        val rs = conn.getMetaData.getPrimaryKeys(null, null, tableName)
        
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

    def extractTables(conn: Connection): Seq[TableModel] = {
        val data: DatabaseMetaData = conn.getMetaData

        val tables: ResultSet = data.getTables(null, "%", "%", List("TABLE").toArray)

        var returnTables = List[TableModel]()

        while (tables.next) {
            val tableName = tables.getString("TABLE_NAME")
            val tableModel = extractTable(tableName, conn)
            returnTables += tableModel
        }
        
        returnTables
    }
    
    def extractTables(conn: ManagedResource[Connection]): Seq[TableModel] = for (c <- conn) yield extractTables(c)
    
    def extract(conn: Connection): DatabaseModel =
        new DatabaseModel("xx", extractTables(conn))
    
    def extract(conn: ManagedResource[Connection]): DatabaseModel =
        for (c <- conn) yield extract(c)
    
    def search(url: String): Seq[TableModel] = {
        extractTables(connection(url))
    }


    def parse(jdbcUrl: String): DatabaseModel = new DatabaseModel("database", search(jdbcUrl))
    
    def parseTable(tableName: String, jdbcUrl: String) =
        for (c <- connection(jdbcUrl)) yield extractTable(tableName, c)
    
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
    
    def extractTable(name: String) = for (c <- conn) yield JdbcModelExtractor.extractTable(name, c)
    
    "Simple Table" in {
        dropTable("bananas")
        execute("CREATE TABLE bananas (id INT, color VARCHAR(100), PRIMARY KEY(id))")
        
        val table = for (c <- conn) yield JdbcModelExtractor.extractTable("bananas", c)
        
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
        
        val table = for (c <- conn) yield JdbcModelExtractor.extractTable("users", c)
        
        //println(table.keys)
        
        val ageK = table.indexes.find(_.name.get == "age_k").get
        assert(List("age") == ageK.columns.toList)
        assert(false == ageK.isUnique)
        
        val firstLastK = table.indexWithColumns("first_name", "last_name")
        assert(true == firstLastK.isUnique)
        
        val ageLastK = table.indexWithColumns("age", "last_name")
        assert(false == ageLastK.isUnique)
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
}

// vim: set ts=4 sw=4 et:
