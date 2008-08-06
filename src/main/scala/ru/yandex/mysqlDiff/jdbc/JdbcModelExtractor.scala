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
    
    private def groupBy[A, B](seq: Seq[A])(f: A => B): Seq[(B, Seq[A])] = {
        def g(seq: Seq[(B, A)]): Seq[(B, Seq[A])] = seq match {
            case Seq() => List()
            case Seq((b, a)) => List((b, List(a)))
            case Seq((b1, a1), rest @ _*) =>
                g(rest) match {
                    case Seq((`b1`, l), rest @ _*) => (b1, a1 :: l.toList) :: rest.toList
                    case r => (b1, List(a1)) :: r.toList
                }
        }
            
        g(seq.map(a => (f(a), a)))
    }
    
    object MetaDao {
        /** INFORMATION_SCHEMA.COLUMNS */
        case class MysqlColumnInfo(
            tableCatalog: String, tableSchema: String, tableName: String,
            columnName: String, ordinalPosition: Int, columnDefault: String,
            isNullable: Boolean, dataType: String,
            characterMaximumLength: Double, characterOctetLength: Double,
            numericPrecision: Int, numericScale: Int,
            characterSetName: String, collationName: String,
            columnType: String, /* skipped some columns */ columnComment: String
        )
        
        private def mapColumnsRow(rs: ResultSet) = {
            import rs._
            MysqlColumnInfo(
                getString("table_catalog"), getString("table_schema"), getString("table_name"),
                getString("column_name"), getInt("ordinal_position"), getString("column_default"),
                getBoolean("is_nullable"), getString("data_type"),
                getDouble("character_maximum_length"), getDouble("character_octet_length"),
                getInt("numeric_precision"), getInt("numeric_scale"),
                getString("character_set_name"), getString("collation_name"),
                getString("column_type"), getString("column_comment")
            )
        }
        
    }
    
    import MetaDao._
        
    class MetaDao(conn: Connection) {
        
        def findPrimaryKey(catalog: String, schema: String, tableName: String): Option[PrimaryKeyModel] = {
            val rs = conn.getMetaData.getPrimaryKeys(catalog, schema, tableName)
            
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
                
                Some(new PrimaryKeyModel(pkNameO, r.map(_.columnName)))
            }
        }
        
        def findTableNames(catalog: String, schema: String) = {
            val data = conn.getMetaData

            val rs = data.getTables(catalog, schema, "%", List("TABLE").toArray)

            read(rs) { rs =>
                rs.getString("TABLE_NAME")
            }
        }
        
        // regular indexes
        def findIndexes(catalog: String, schema: String, tableName: String): Seq[IndexModel] = {
            val rs = conn.getMetaData.getIndexInfo(catalog, schema, tableName, false, false)
            
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
        
        def findImportedKeys(catalog: String, schema: String, tableName: String): Seq[ForeignKeyModel] = {
            val rs = conn.getMetaData.getImportedKeys(catalog, schema, tableName)
            
            case class R(keyName: String, externalTableName: String,
                    localColumnName: String, externalColumnName: String)
            
            val r = read(rs) { rs =>
                R(rs.getString("FK_NAME"), rs.getString("PKTABLE_NAME"),
                        rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME"))
            }
            
            // key name can be null
            val keys = Set(r.map(x => (x.keyName, x.externalTableName)): _*).toSeq
            
            keys.map { case (keyName, _) =>
                val rows = r.filter(_.keyName == keyName)
                val externalTableNames = Set(rows.map(_.externalTableName): _*)
                if (externalTableNames.size != 1) {
                    val m = "internal error, got external table names: " + externalTableNames +
                            " for key " + keyName
                    throw new IllegalStateException(m)
                }
                ForeignKeyModel(Some(keyName), rows.map(_.localColumnName),
                        externalTableNames.elements.next, rows.map(_.externalColumnName))
            }
        }
        
        def mapTableOptions(rs: ResultSet) =
            (rs.getString("TABLE_NAME"), List(TableOption("ENGINE", rs.getString("ENGINE"))))
        
        def findTablesOptions(schema: String): Seq[(String, Seq[TableOption])] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, schema)
            val rs = ps.executeQuery()
            read(rs)(mapTableOptions _)
        }
        
        def findTableOptions(schema: String, tableName: String): Seq[TableOption] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, schema)
            ps.setString(2, tableName)
            val rs = ps.executeQuery()
            rs.next()
            mapTableOptions(rs)._2
        }
        
        def findMysqlTablesColumns(schema: String): Seq[(String, Seq[MysqlColumnInfo])] = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? ORDER BY table_name"
            val ps = conn.prepareStatement(q)
            ps.setString(1, schema)
            val rs = ps.executeQuery()
            
            val columns = read(rs)(mapColumnsRow _)
            
            groupBy[MysqlColumnInfo, String](columns)(_.tableName)
        }
        
        def findMysqlColumns(schema: String, tableName: String) = {
            val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
            val ps = conn.prepareStatement(q)
            ps.setString(1, schema)
            ps.setString(2, tableName)
            val rs = ps.executeQuery()
            
            read(rs)(mapColumnsRow _)
        }
        
    }
    
    abstract class SchemaExtractor(conn: Connection) {
        protected val dao = new MetaDao(conn)
        
        lazy val currentDb = {
            val db = conn.getMetaData.getURL.replaceFirst("\\?.*", "").replaceFirst(".*/", "")
            require(db.length > 0)
            db
        }
        
        def currentCatalog = currentDb
        def currentSchema: String = null
        
        def extractTable(tableName: String): TableModel = {
            val data = conn.getMetaData
            val columns = data.getColumns(currentCatalog, currentSchema, tableName, "%")
            
            val mysqlColumns = getMysqlColumns(tableName)
            
            val columnsList = read(columns) { columns =>
                val colName = columns.getString("COLUMN_NAME")
                
                val colType = columns.getString("TYPE_NAME")
                
                def getIntOption(rs: ResultSet, columnName: String) = {
                    val v = rs.getInt(columnName)
                    if (rs.wasNull) None
                    else Some(v)
                }
                
                val colTypeSize = getIntOption(columns, "COLUMN_SIZE")
                    .filter(x => DataType.base(colType).isLengthAllowed)

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
                
                val mysqlColumn = mysqlColumns.find(_.columnName == colName).get

                val defaultValueFromDb =
                    // http://bugs.mysql.com/36699
                    if (true) mysqlColumn.columnDefault
                    else columns.getString("COLUMN_DEF")
                
                val isUnsigned = false
                val isZerofill = false
                val characterSet = Some(mysqlColumn.characterSetName)
                    .filter(x => x != null && x != "")
                    .map(MysqlCharacterSet(_))
                val collate = Some(mysqlColumn.collationName)
                    .filter(x => x != null && x != "")
                    .map(MysqlCollate(_))
                
                val dataType = DataType(colType, colTypeSize, Nil ++ characterSet ++ collate)
                
                val defaultValue = parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))
                
                val props = new ColumnProperties(List[ColumnProperty]() ++ nullable ++ defaultValue ++ autoIncrement)
                new ColumnModel(colName, dataType, props)
            }
            
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
        
        def getMysqlColumns(tableName: String): Seq[MysqlColumnInfo]
    }
    
    class SingleTableSchemaExtractor(conn: Connection) extends SchemaExtractor(conn) {
        override def getTableOptions(tableName: String) =
            dao.findTableOptions(currentDb, tableName)
        
        override def getMysqlColumns(tableName: String) =
            dao.findMysqlColumns(currentDb, tableName)
    }
    
    class AllTablesSchemaExtractor(conn: Connection) extends SchemaExtractor(conn) {
    
        def extract(): DatabaseModel =
            new DatabaseModel(extractTables())
        
        private val cachedTableNames = new Lazy(dao.findTableNames(currentCatalog, currentSchema))
        def tableNames = cachedTableNames.get
        
        private val cachedTablesOptions = new Lazy(dao.findTablesOptions(currentDb))
        
        def getTableOptions(tableName: String): Seq[TableOption] =
            cachedTablesOptions.get.find(_._1 == tableName).get._2
        
        val cachedMysqlColumnDefaultValues = new Lazy(dao.findMysqlTablesColumns(currentDb))
        
        def getMysqlColumns(tableName: String) =
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
        else if (dataType.isAnyNumber) {
            Some(script.parser.SqlParserCombinator.parseValue(s))
        }
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


    def parse(jdbcUrl: String): DatabaseModel = new DatabaseModel(search(jdbcUrl))
    
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
                new DatabaseModel(List(parseTable(tableName, jdbcUrl)))
            case _ =>
                usage(); exit(1)
    	}
    	
    	print(ModelSerializer.serializeDatabaseToText(model))
    }
}

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
    
    "table options" in {
        dropTable("dogs")
        execute("CREATE TABLE dogs (id INT) ENGINE=InnoDB")
        val table = extractTable("dogs")
        table.options must contain(TableOption("ENGINE", "InnoDB"))
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
    
    "fetches CHARACTER SET and COLLATE" in {
        dropTable("qwqw")
        execute("CREATE TABLE qwqw (a VARCHAR(2), b VARCHAR(2) CHARACTER SET utf8 COLLATE utf8_bin)")
        
        val table = extractTable("qwqw")
        val a = table.column("a")
        val b = table.column("b")
        
        b.dataType.options must contain(MysqlCharacterSet("utf8"))
        b.dataType.options must contain(MysqlCollate("utf8_bin"))
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
