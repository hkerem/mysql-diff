package ru.yandex.mysqlDiff.vendor.mysql

import java.sql._

import jdbc._
import util._
import model._

import MetaDao._

import Implicits._

object MysqlMetaDao {
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

import MysqlMetaDao._

/**
 * MySQL specific implementation of MetaDao. Uses INFORMATION_SCHEMA
 */
class MysqlMetaDao(jt: JdbcTemplate) extends MetaDao(jt) {
    import MetaDao._
    
    import jt._
    
    // MySQL does not store default charset, collation only
    // http://dev.mysql.com/doc/refman/5.1/en/tables-table.html
    def mapTableOptions(rs: ResultSet) =
        (rs.getString("TABLE_NAME"), Seq(
                MysqlEngineTableOption(rs.getString("ENGINE")),
                MysqlCollateTableOption(rs.getString("TABLE_COLLATION"))
                ))
    
    def findMysqlTablesOptions(schema: String): Seq[(String, Seq[TableOption])] = {
        val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?"
        query(q, schema).seq(mapTableOptions _)
    }
    
    def findMysqlTableOptions(schema: String, tableName: String): Seq[TableOption] = {
        val q = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?"
        query(q, schema, tableName).single(mapTableOptions _)._2
    }
    
    override def findTablesOptions(catlog: String, schema: String): Seq[(String, Seq[TableOption])] =
        findMysqlTablesOptions(schema)
    
    override def findTableOptions(catalog: String, schema: String, tableName: String): Seq[TableOption] =
        findMysqlTableOptions(schema, tableName)
    
    /** Supermagic utility, even author does not understand how it works */
    protected def groupBy[A, B](seq: Seq[A])(f: A => B): Seq[(B, Seq[A])] = {
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
    
    def findMysqlTablesColumns(catalog: String, schema: String): Seq[(String, Seq[MysqlColumnInfo])] = {
        val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? ORDER BY table_name"
        val columns = query(q, schema).seq(mapColumnsRow _)
        groupBy[MysqlColumnInfo, String](columns)(_.tableName)
    }
    
    def findMysqlTableColumns(catalog: String, schema: String, tableName: String) = {
        val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
        query(q, schema, tableName).seq(mapColumnsRow _)
    }

}

object MysqlMetaDaoTests extends DbMetaDaoTests(vendor.mysql.MysqlTestDataSourceParameters.ds) {
}

class MysqlJdbcModelExtractor(context: Context) extends JdbcModelExtractor(context) {
    import context._
    
    override def createMetaDao(jt: JdbcTemplate) = new MysqlMetaDao(jt)
    
    protected class MysqlAllTablesSchemaExtractor(jt: JdbcTemplate) extends AllTablesSchemaExtractor(jt) {
    }
    
    protected class MysqlSingleTableSchemaExtractor(jt: JdbcTemplate) extends SingleTableSchemaExtractor(jt)
    
    trait MysqlSchemaExtractor extends SchemaExtractor {
        
        import jt._
        
        override val dao = createMetaDao(jt) // hack
        
        override def extractTableColumns(tableName: String): Seq[ColumnModel] = metaData { data =>
            // copy-paste of super plus hacks
            
            val columns = data.getColumns(currentCatalog, currentSchema, tableName, "%")
            
            val mysqlColumns = getMysqlColumns(tableName)
            
            read(columns) { columns =>
                var base = parseTableColumn(columns)
                
                val mysqlColumn = mysqlColumns.find(_.columnName == base.name).get

                val defaultValueFromDb =
                    // http://bugs.mysql.com/36699
                    if (true) mysqlColumn.columnDefault
                    else columns.getString("COLUMN_DEF")
                
                // XXX: fetch
                val isUnsigned = false
                val isZerofill = false
                val characterSet = Some(mysqlColumn.characterSetName)
                    .filter(x => x != null && x != "")
                    .map(MysqlCharacterSet(_))
                val collate = Some(mysqlColumn.collationName)
                    .filter(x => x != null && x != "")
                    .map(MysqlCollate(_))
                
                val dataTypeOptions = Seq[DataTypeOption]() ++ characterSet ++ collate
                val dataType = base.dataType.overrideOptions(dataTypeOptions)
                
                val defaultValue = parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))
                
                val props = Seq[ColumnProperty]() ++ defaultValue
                base.overrideProperties(props).withDataType(dataType)
            }
        }
        
        def getMysqlColumns(tableName: String): Seq[MysqlColumnInfo]
    
    }

    protected override def newAllTablesSchemaExtractor(jt: JdbcTemplate) =
        new AllTablesSchemaExtractor(jt) with MysqlSchemaExtractor {
            override def getMysqlColumns(tableName: String) =
                dao.findMysqlTableColumns(currentCatalog, currentSchema, tableName)
        }
    
    protected override def newSingleTableSchemaExtractor(jt: JdbcTemplate) =
        new SingleTableSchemaExtractor(jt) with MysqlSchemaExtractor {
            val cachedMysqlColumns = new Lazy(dao.findMysqlTablesColumns(currentCatalog, currentSchema))
            
            override def getMysqlColumns(tableName: String) =
                cachedMysqlColumns.get.find(_._1 == tableName).get._2
        }

}

object MysqlJdbcModelExtractorTests extends JdbcModelExtractorTests(MysqlContext, MysqlTestDataSourceParameters) {
    import MysqlContext._
    import MysqlTestDataSourceParameters._
    
    import jdbcTemplate.execute
    
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
        
        val firstLastK = table.uniqueKeyWithColumns("first_name", "last_name")
        
        val ageLastK = table.indexWithColumns("age", "last_name")
        ()
    }
    
    "PK is not in indexes list" in {
        dropTable("files")
        execute("CREATE TABLE files (id INT, PRIMARY KEY(id))")
        
        val table = extractTable("files")
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
                "CONSTRAINT fk2c FOREIGN KEY fk2i(pid1, pid2) REFERENCES person(id1, id2)" +
                ") ENGINE=InnoDB")
        
        val citizen = extractTable("citizen")
        val city = extractTable("city")
        val person = extractTable("person")
        
        citizen.foreignKeys must haveSize(2)
        
        val fkc = citizen.foreignKeys.find(_.localColumns.toList == List("city_id")).get
        fkc.localColumns must beLike { case Seq("city_id") => true }
        fkc.externalColumns must beLike { case Seq("id") => true }
        fkc.externalTable must_== "city"
        
        val fkp = citizen.foreignKeys.find(_.localColumns.toList == List("pid1", "pid2")).get
        fkp.localColumns must beLike { case Seq("pid1", "pid2") => true }
        fkp.externalColumns must beLike { case Seq("id1", "id2") => true }
        fkp.externalTable must_== "person"
        fkp.name must_== Some("fk2c")
        
        citizen.indexes must haveSize(2)
        val ip = citizen.indexes.find(_.columns.toList == List("pid1", "pid2")).get
        ip.name must_== Some("fk2i")
        
        city.foreignKeys must haveSize(0)
        person.foreignKeys must haveSize(0)
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
