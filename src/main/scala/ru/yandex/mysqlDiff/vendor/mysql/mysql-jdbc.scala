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

class MysqlMetaDao(jt: JdbcTemplate) extends MetaDao(jt) {
    import MetaDao._
    
    import jt._
    
    // MySQL does not store default charset:
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
    
    trait MysqlSchemaExtrator extends SchemaExtractor {
        
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
        new AllTablesSchemaExtractor(jt) with MysqlSchemaExtrator {
            override def getMysqlColumns(tableName: String) =
                dao.findMysqlTableColumns(currentCatalog, currentSchema, tableName)
        }
    
    protected override def newSingleTableSchemaExtractor(jt: JdbcTemplate) =
        new SingleTableSchemaExtractor(jt) with MysqlSchemaExtrator {
            val cachedMysqlColumns = new Lazy(dao.findMysqlTablesColumns(currentCatalog, currentSchema))
            
            def getMysqlColumns(tableName: String) =
                cachedMysqlColumns.get.find(_._1 == tableName).get._2
        }

}

// vim: set ts=4 sw=4 et:
