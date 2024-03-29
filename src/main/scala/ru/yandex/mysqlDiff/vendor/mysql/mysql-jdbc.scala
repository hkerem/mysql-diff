package ru.yandex.mysqlDiff
package vendor.mysql

import java.sql._

import ru.yandex.small.jdbc._

import jdbc._
import util._
import model._

import MetaDao._

import Implicits._

object MysqlMetaDao {
    /** INFORMATION_SCHEMA.COLUMNS */
    case class MysqlColumnInfo(
        tableCatalog: String, tableSchema: String, tableName: String,
        columnName: String, ordinalPosition: Long, columnDefault: String,
        isNullable: Boolean, dataType: String,
        characterMaximumLength: Long, characterOctetLength: Double,
        numericPrecision: Long, numericScale: Long,
        characterSetName: String, collationName: String,
        columnType: String, /* skipped some columns */ columnComment: String
    )
    
    private def mapColumnsRow(rs: ResultSet) = {
        import rs._
        MysqlColumnInfo(
            getString("table_catalog"), getString("table_schema"), getString("table_name"),
            getString("column_name"), getLong("ordinal_position"), getString("column_default"),
            getBoolean("is_nullable"), getString("data_type"),
            getLong("character_maximum_length"), getDouble("character_octet_length"),
            getLong("numeric_precision"), getLong("numeric_scale"),
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
    
    // http://bugs.mysql.com/36699
    private val PROPER_COLUMN_DEF_MIN_MYSQL_VERSION = MysqlServerVersion.parse("5.0.51")
    
    // MySQL does not store default charset, collation only
    // http://dev.mysql.com/doc/refman/5.1/en/tables-table.html
    def mapTableOptions(rs: ResultSet) =
        (rs.getString("TABLE_NAME"), Seq(
                MysqlEngineTableOption(rs.getString("ENGINE")),
                MysqlCollateTableOption(rs.getString("TABLE_COLLATION")),
                // stupid MySQL developers print some left information in the TABLE_COMMENT column
                MysqlCommentTableOption(rs.getString("TABLE_COMMENT").replaceFirst("(; |^)InnoDB free: .*", ""))
                ))

    private val INFORMATION_SCHEMA_TABLES_FIELDS = Seq("TABLE_NAME", "ENGINE", "TABLE_COLLATION", "TABLE_COMMENT")
    
    def findMysqlTablesOptions(schema: String): Seq[(String, Seq[TableOption])] = {
        val q = "SELECT " + INFORMATION_SCHEMA_TABLES_FIELDS.mkString(", ") + " FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?"
        query(q, schema).seq(mapTableOptions _)
    }
    
    def findMysqlTableOptions(schema: String, tableName: String): Seq[TableOption] = {
        val q = "SELECT " + INFORMATION_SCHEMA_TABLES_FIELDS.mkString(", ") + " FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?"
        query(q, schema, tableName).single(mapTableOptions _)._2
    }
    
    override def findTablesOptions(catlog: String, schema: String): Seq[(String, Seq[TableOption])] =
        findMysqlTablesOptions(schema)
    
    override def findTableOptions(catalog: String, schema: String, tableName: String): Seq[TableOption] =
        findMysqlTableOptions(schema, tableName)
    
    protected def groupBy[A, B](seq: Seq[A])(f: A => B): Seq[(B, Seq[A])] = {
        import scala.collection.mutable._
        val result = new HashMap[B, ListBuffer[A]]()
        for (a <- seq) {
            val list = result.getOrElseUpdate(f(a), new ListBuffer[A]())
            list += a
        }
        result.toSeq
    }
    
    def findMysqlTablesColumns(catalog: String, schema: String): Seq[(String, Seq[MysqlColumnInfo])] = {
        Validate.notNull(schema)
        val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? ORDER BY table_name"
        val columns = query(q, schema).seq(mapColumnsRow _)
        groupBy[MysqlColumnInfo, String](columns)(_.tableName)
    }
    
    def findMysqlTableColumns(catalog: String, schema: String, tableName: String) = {
        Validate.notNull(schema, "schema")
        Validate.notNull(tableName, "tableName")
        val q = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
        query(q, schema, tableName).seq(mapColumnsRow _) match {
            case Seq() => throw new MysqlDiffException(
                "no columns found in schema "+ schema +", table "+ tableName +"")
            case l => l
        }
    }
    
    protected override def mapIndexInfoRowToIndexColumn(row: IndexInfoRow) =
        IndexColumn(row.columnName, row.ascOrDesc.getOrElse(true), row.vendorSpecific.asInstanceOf[Option[Int]])

    protected override def findIndexInfoRows(catalog: String, schema: String, tableName: String) =
        jt.query("SHOW INDEX FROM " + tableName).seq { rs =>
            IndexInfoRow(rs.getString("KEY_NAME"), rs.getBoolean("NON_UNIQUE"),
                rs.getInt("SEQ_IN_INDEX"), rs.getString("COLUMN_NAME"),
                mapAscOrDesc(rs.getString("COLLATION")), rs.getIntOption("SUB_PART"))
        }
    
    protected override def mapPrimaryKeyInfoRowToPrimaryKeyColumn(row: PrimaryKeyInfoRow) = {
        // XXX: primary key should not store indexing info, implicit (or explicit) index should
        val (ascOrDesc, subPart) =
            row.vendorSpecific.asInstanceOf[(Option[Boolean], Option[Int])]
        IndexColumn(row.columnName, ascOrDesc.getOrElse(true), subPart)
    }
    
    protected override def findPrimaryKeyInfoRows(catalog: String, schema: String, tableName: String) =
        (jt.query("SHOW INDEX FROM " + tableName).seq { rs =>
            val keyName = rs.getString("KEY_NAME")
            keyName match {
                case "PRIMARY" =>
                    Some(PrimaryKeyInfoRow(keyName, rs.getString("COLUMN_NAME"), rs.getInt("SEQ_IN_INDEX"),
                        (mapAscOrDesc(rs.getString("COLLATION")), rs.getIntOption("SUB_PART"))))
                case _ => None
            }
        }).flatMap((x: Option[PrimaryKeyInfoRow]) => x)
}

object MysqlMetaDaoTests extends DbMetaDaoTests(vendor.mysql.MysqlTestDataSourceParameters.ds) {
}

class MysqlJdbcModelExtractor(connectedContext: MysqlConnectedContext)
    extends JdbcModelExtractor(connectedContext)
{
    import connectedContext._
    import context._
    
    override def useParserToParseDefaultValue = false
    
    override def urlDbIsCatalog = false
    
    protected class MysqlAllTablesSchemaExtractor extends AllTablesSchemaExtractor
    
    protected class MysqlSingleTableSchemaExtractor extends SingleTableSchemaExtractor
    
    trait MysqlSchemaExtractor extends SchemaExtractor {
        
        import jt._
        
        override def extractTableColumns(tableName: String): Seq[ColumnModel] = metaData { data =>
            // copy-paste of super plus hacks
            
            val columns = data.getColumns(currentCatalog, currentSchema, tableName, "%")
            
            val mysqlColumns = getMysqlColumns(tableName)
            
            columns.read { columns =>
                val base = parseTableColumn(columns)
                
                val mysqlColumn = mysqlColumns.find(_.columnName == base.name).get

                val defaultValueFromDb =
                    // http://bugs.mysql.com/36699
                    if (true) mysqlColumn.columnDefault
                    else columns.getString("COLUMN_DEF")
                
                lazy val characterSet = Some(mysqlColumn.characterSetName)
                    .filter(x => x != null && x != "")
                lazy val collate = Some(mysqlColumn.collationName)
                    .filter(x => x != null && x != "")
                
                lazy val DataTypeWithLength(_, length) = base.dataType
                
                val columnType = mysqlColumn.dataType.toUpperCase
                val dataType =
                    if (columnType.toUpperCase.matches("(ENUM|SET)\\b.*")) {
                        MysqlParserCombinator.parseDataType(mysqlColumn.columnType)
                    } else if (MysqlDataTypes.characterDataTypeNames.contains(columnType)) {
                        val length = mysqlColumn.characterMaximumLength match {
                            case x if x <= 0 || x >= Int.MaxValue => None
                            case x => Some(x.toInt)
                        }
                        
                        new MysqlCharacterDataType(columnType, length, characterSet, collate)
                    } else if (MysqlDataTypes.textDataTypeNames.contains(columnType)) {
                        new MysqlTextDataType(columnType, None, characterSet, collate)
                    } else if (MysqlDataTypes.numericDataTypeNames.contains(columnType)) {
                        // XXX: fetch unsigned, zerofill
                        MysqlParserCombinator.parseDataType(mysqlColumn.columnType)
                    } else {
                        base.dataType
                    }
                        
                val defaultValue = parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))
                
                val props = Seq[ColumnProperty]() ++ defaultValue ++ Some(MysqlComment(mysqlColumn.columnComment))
                base.overrideProperties(props).copy(dataType=dataType)
            }
        }
        
        def getMysqlColumns(tableName: String): Seq[MysqlColumnInfo]
    
    }

    protected override def newAllTablesSchemaExtractor() =
        new AllTablesSchemaExtractor with MysqlSchemaExtractor {
            val cachedMysqlColumns = new Lazy(metaDao.findMysqlTablesColumns(currentCatalog, currentSchema))
            
            override def getMysqlColumns(tableName: String) =
                cachedMysqlColumns.get.find(_._1 == tableName) match {
                    case Some(x) => x._2
                    case None => throw new MysqlDiffException("no definition for table: " + tableName)
                }
        }
    
    protected override def newSingleTableSchemaExtractor() =
        new SingleTableSchemaExtractor with MysqlSchemaExtractor {
            override def getMysqlColumns(tableName: String) =
                metaDao.findMysqlTableColumns(currentCatalog, currentSchema, tableName)
        }

}

object MysqlJdbcModelExtractorTests
    extends JdbcModelExtractorTests(MysqlTestDataSourceParameters.connectedContext)
{
    import MysqlTestDataSourceParameters.connectedContext._
    import MysqlTestDataSourceParameters.connectedContext.context._
    
    import jt.execute
    
    "Simple Table" in {
        dropTable("bananas")
        execute("CREATE TABLE bananas (id INT, color VARCHAR(100), PRIMARY KEY(id))")
        
        val table = extractTable("bananas")
        
        assert("bananas" == table.name)
        
        assert("id" == table.columns(0).name)
        assert("INT" == table.columns(0).dataType.name)
        
        assert("color" == table.columns(1).name)
        table.columns(1).dataType must beLike {
            case MysqlCharacterDataType("VARCHAR", Some(100), _, _) => true
            case _ => false
        }
        
        table.primaryKey.get.columnNames.toList must_== List("id")
    }
    
    "Indexes" in {
        dropTable("users")
        execute("CREATE TABLE users (first_name VARCHAR(20), last_name VARCHAR(20), age INT, INDEX age_k(age), UNIQUE KEY(first_name, last_name), KEY(age, last_name))")
        
        val table = extractTable("users")
        
        val ageK = table.indexes.find(_.name.get == "age_k").get
        List("age") must_== ageK.columnNames.toList
        
        val firstLastK = table.uniqueKeyWithColumns("first_name", "last_name")
        
        val ageLastK = table.indexWithColumns("age", "last_name")
        ()
    }
    
    "INDEX column part" in {
        ddlTemplate.recreateTable(
            "CREATE TABLE index_c_p (c1 VARCHAR(100), c2 VARCHAR(50), INDEX ind (c1(10), c2)) ENGINE=InnoDB")
        val table = extractTable("index_c_p")
        table.indexes must haveSize(1)
        val index = table.indexes.head
        index.columns must haveSize(2)
        index.columns(0).length must_== Some(10)
        index.columns(1).length must_== None
        ()
    }
    
    "PRIMARY KEY INDEX column part" in {
        ddlTemplate.recreateTable(
            "CREATE TABLE pk_lp_extr (name VARCHAR(20), PRIMARY KEY (name(3)))")
        val table = extractTable("pk_lp_extr")
        table.primaryKey.get.columns.head must_== IndexColumn("name", true, Some(3))
    }
    
    "PK is not in indexes list" in {
        dropTable("files")
        execute("CREATE TABLE files (id INT, PRIMARY KEY(id))")
        
        val table = extractTable("files")
        table.primaryKey.get.columnNames.toList must_== List("id")
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
        citizen.indexes must haveSize(3)
        
        val fkc = citizen.foreignKeys.find(_.localColumnNames.toList == List("city_id")).get
        fkc.localColumnNames.toList must_== List("city_id")
        fkc.externalColumns must beLike { case Seq("id") => true }
        fkc.externalTable must_== "city"
        val ic = citizen.indexes.find(_.columnNames.toList == List("city_id"))
        
        val fkp = citizen.foreignKeys.find(_.localColumnNames.toList == List("pid1", "pid2")).get
        fkp.localColumnNames.toList must_== List("pid1", "pid2")
        fkp.externalColumns must beLike { case Seq("id1", "id2") => true }
        fkp.externalTable must_== "person"
        fkp.name must_== Some("fk2c")
        val ip = citizen.indexes.find(_.columnNames.toList == List("pid1", "pid2")).get
        // behavior is different in mysql 5.0 and 5.1
        // http://bitbucket.org/stepancheg/mysql-diff/wiki/EvilMysql/ForeignKeyConstraint
        Seq("fk2i", "fk2c") must contain(ip.name.get)
        
        city.foreignKeys must haveSize(0)
        person.foreignKeys must haveSize(0)
    }
    
    "FOREIGN KEY actions" in {
        dropTable("ggg")
        dropTable("rrr")
        execute("CREATE TABLE rrr (id INT PRIMARY KEY) ENGINE=InnoDB")
        for (updateDelete <- List(true, false)) {
            for (action <- List(ImportedKeyNoAction, ImportedKeyCascade, ImportedKeySetNull)) {
                dropTable("ggg")
                val text =
                    (if (updateDelete) "ON UPDATE"
                    else "ON DELETE") +
                    " " +
                    (action match {
                        case ImportedKeyNoAction => "NO ACTION"
                        case ImportedKeyCascade => "CASCADE"
                        case ImportedKeySetNull => "SET NULL"
                    })
                    
                execute("CREATE TABLE ggg (r_id INT, FOREIGN KEY (r_id) REFERENCES rrr(id) " + text + ") ENGINE=InnoDB")
                
                val table = extractTable("ggg")
                val Seq(fk) = table.foreignKeys
                
                val gotRule = if (updateDelete) fk.updateRule else fk.deleteRule
                gotRule must_== Some(action)
            }
        }
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
    
    "fetch TABLE COMMENT MyISAM" in {
        ddlTemplate.recreateTable("CREATE TABLE table_comment_fetch_myisam (id INT) COMMENT='stone' ENGINE=MyISAM")
        val table = extractTable("table_comment_fetch_myisam")
        table.options.properties must contain(MysqlCommentTableOption("stone"))
    }
    
    "fetch TABLE COMMENT InnoDB" in {
        ddlTemplate.recreateTable("CREATE TABLE table_comment_fetch_innodb (id INT) COMMENT='stone' ENGINE=InnoDB")
        val table = extractTable("table_comment_fetch_innodb")
        table.options.properties must contain(MysqlCommentTableOption("stone"))
    }
    
    "fetch TABLE empty COMMENT InnoDB" in {
        ddlTemplate.recreateTable("CREATE TABLE table_comment_fetch_innodb_empty (id INT) ENGINE=InnoDB")
        val table = extractTable("table_comment_fetch_innodb_empty")
        table.options.properties must contain(MysqlCommentTableOption(""))
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
    
    "various types" in {
        dropTable("various_types")
        execute("CREATE TABLE various_types (t TEXT, lt LONGTEXT, v VARCHAR(100))")
        val table = extractTable("various_types")
        ()
    }
    
    "unspecified AUTO_INCREMENT" in {
        dropTable("ships")
        execute("CREATE TABLE ships (id INT NOT NULL, name VARCHAR(10), PRIMARY KEY(id))")
        
        val t = extractTable("ships")
        t.column("id").properties.find(MysqlAutoIncrementPropertyType) must_== Some(MysqlAutoIncrement(false))
        //t.column("name").properties.autoIncrement must_== None
    }
    
    "COLUMN CHARACTER SET and COLLATE" in {
        dropTable("qwqw")
        execute("CREATE TABLE qwqw (a VARCHAR(2), b VARCHAR(2) CHARACTER SET utf8 COLLATE utf8_bin)")
        
        val table = extractTable("qwqw")
        val a = table.column("a")
        val b = table.column("b")
        
        b.dataType must beLike {
            case MysqlCharacterDataType("VARCHAR", Some(2), Some("utf8"), Some("utf8_bin")) => true
            case _ => false
        }
    }
    
    "ENUM" in {
        ddlTemplate.recreateTable(
            "CREATE TABLE s_ev (season ENUM('winter', 'spring', 'summer', 'autumn')," +
            "   probability ENUM('yes', 'no', 'maybe', 'definitely') DEFAULT 'yes')")
        val table = extractTable("s_ev")
        table.column("season").dataType must_== new MysqlEnumDataType(Seq("winter", "spring", "summer", "autumn"))
        table.column("season").defaultValue must_== Some(NullValue)
        table.column("probability").defaultValue must_== Some(StringValue("yes"))
    }
    
    "BOOLEAN" in {
        ddlTemplate.recreateTable(
            "CREATE TABLE t_b (a BOOLEAN)")
        val t = extractTable("t_b")
        t.column("a").dataType must beLike {
            case MysqlNumericDataType("TINYINT", Some(1), _, _, _) => true
            case _ => false
        }
    }
    
    "BIT DEFAULT b'0'" in {
        ddlTemplate.recreateTable(
            "CREATE TABLE bit_with_default_0 (aux BIT NOT NULL DEFAULT b'0') ENGINE=InnoDB CHARSET=latin1")
        val t = extractTable("bit_with_default_0")
        val c = t.column("aux")
        c.defaultValue must_== Some(NumberValue(0))
    }
    
    "COLUMN COMMENT" in {
        dropTable("beers")
        execute("CREATE TABLE beers (name VARCHAR(100) COMMENT 'short name')")
        val t = extractTable("beers")
        val c = t.column("name")
        c.properties.find(MysqlCommentPropertyType) must_== Some(MysqlComment("short name"))
    }

}

// vim: set ts=4 sw=4 et:
