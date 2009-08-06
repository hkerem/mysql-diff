package ru.yandex.mysqlDiff.jdbc

import java.sql._
import util._

import misc.jdbc._

import model._

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

import vendor.mysql._

import Implicits._

/**
 * DAO to query database information.
 */
class MetaDao(jt: JdbcTemplate) {
    def this(ds: () => Connection) = this(new JdbcTemplate(ds))
    def this(ds: LiteDataSource) = this(new JdbcTemplate(ds))
    
    import jt._
    
    import MetaDao._
    
    def findPrimaryKey(catalog: String, schema: String, tableName: String): Option[PrimaryKeyModel] =
        execute { conn =>
            val rs = conn.getMetaData.getPrimaryKeys(catalog, schema, tableName)

            case class R(pkName: String, columnName: String, keySeq: Int)

            val r0 = rs.read { rs =>
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

                // XXX: IndexColumn: asc, length
                Some(new PrimaryKeyModel(pkNameO, r.map(x => IndexColumn(x.columnName))))
            }
        }
    
    /**
     * Find all tables in specified catalog and schema.
     */
    def findTableNames(catalog: String, schema: String) = execute { conn =>
        val data = conn.getMetaData
        
        val rs = data.getTables(catalog, schema, "%", List("TABLE").toArray)
        
        rs.read { rs =>
            rs.getString("TABLE_NAME")
        }
    }
    
    /**
     * Find all sequences in specified catalog and schema.
     */
    def findSequenceNames(catalog: String, schema: String): Seq[String] =
        // not all databases support sequences
        Seq()
    
    // regular indexes
    def findIndexes(catalog: String, schema: String, tableName: String): Seq[UniqueOrIndexModel] = execute { conn =>
        val rs = conn.getMetaData.getIndexInfo(catalog, schema, tableName, false, false)
        
        case class R(indexName: String, nonUnique: Boolean, ordinalPosition: Int,
                columnName: String, ascOrDesc: String)
        {
            def unique = !nonUnique
        }
        
        val r = rs.read { rs =>
            R(rs.getString("INDEX_NAME"), rs.getBoolean("NON_UNIQUE"), rs.getInt("ORDINAL_POSITION"),
                    rs.getString("COLUMN_NAME"), rs.getString("ASC_OR_DESC"))
        }
        
        val indexNames = r.map(_.indexName).unique.toSeq
        
        indexNames.map { indexName =>
            val rowsWithName = r.filter(_.indexName == indexName)
            val rows = stableSort(rowsWithName, (r: R) => r.ordinalPosition)
            
            val unique = rows.first.unique
            val columnNames = rows.map(_.columnName)
            
            // XXX: asc, length for IndexColumn
            if (unique) new UniqueKeyModel(Some(indexName), columnNames.map(IndexColumn(_)))
            else new IndexModel(Some(indexName), columnNames.map(IndexColumn(_)), true)
        }
    }
    
    def findImportedKeyModels(catalog: String, schema: String, tableName: String): Seq[ForeignKeyModel] = {
        val r = findImportedKeys(catalog, schema, tableName)

        // key name can be null
        val keys = r.map(x => (x.fkName, x.pkTableName)).unique.toSeq

        keys.map { case (keyName, _) =>
            val rows = r.filter(_.fkName == keyName)
            val externalTableNames = rows.map(_.pkTableName).unique
            if (externalTableNames.size != 1) {
                val m = "internal error, got external table names: " + externalTableNames +
                        " for key " + keyName
                throw new IllegalStateException(m)
            }
            
            // XXX: asc, length for IndexColumn
            ForeignKeyModel(keyName, rows.map(x => IndexColumn(x.fkColumnName)),
                    externalTableNames.elements.next, rows.map(_.pkColumnName),
                    Some(rows.first.updateRule), Some(rows.first.deleteRule))
        }
    }
    
    def findImportedKeys(catalog: String, schema: String, table: String) =
        metaData(_.getImportedKeys(catalog, schema, table).read(mapCrossReference _))
    
    def findExportedKeys(catalog: String, schema: String, table: String) =
        metaData(_.getExportedKeys(catalog, schema, table).read(mapCrossReference _))
    
    def findCrossReference(
            parentCatalog: String, parentSchema: String, parentTable: String,
            foreignCatalog: String, foreignSchema: String, foreignTable: String)
        = metaData(_.getCrossReference(
                parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable)
                        .read(mapCrossReference _))
    
    /**
     * To be overriden in subclasses.
     * @return empty Seqs in this implementation
     */
    def findTablesOptions(catalog: String, schema: String): Seq[(String, Seq[TableOption])] = {
        findTableNames(catalog, schema).map(tn => (tn, findTableOptions(catalog, schema, tn)))
    }
   
    /**
     * To be overriden in subclasses.
     * @return empty Seq in this implementation
     */
    def findTableOptions(catalog: String, schema: String, tableName: String): Seq[TableOption] = {
        Seq()
    }
    

}

object MetaDao {
    import DatabaseMetaData._
    
    def translatePolicy(value: Int) = value match {
        case `importedKeyCascade` => ImportedKeyCascade
        case `importedKeyRestrict` => ImportedKeyNoAction
        case `importedKeyNoAction` => ImportedKeyNoAction
        case `importedKeySetDefault` => ImportedKeySetDefault
        case `importedKeySetNull` => ImportedKeySetNull
    }
    
    def translateDeferrability(value: Int) = value match {
        case `importedKeyInitiallyDeferred` => ImportedKeyInitiallyDeferred
        case `importedKeyInitiallyImmediate` => ImportedKeyInitiallyImmediate
        case `importedKeyNotDeferrable` => ImportedKeyNotDeferrable
    }
    
    case class CrossReference(
        pkTableCatalog: Option[String], pkTableSchema: Option[String], pkTableName: String, pkColumnName: String,
        fkTableCatalog: Option[String], fkTableSchema: Option[String], fkTableName: String, fkColumnName: String,
        keySeq: Int, updateRule: ImportedKeyRule, deleteRule: ImportedKeyRule,
        fkName: Option[String], pkName: Option[String], deferrability: ImportedKeyDeferrability)
    
    def mapCrossReference(rs: ResultSet) = {
        import rs._
        CrossReference(
            rs.getStringOption("PKTABLE_CAT"), rs.getStringOption("PKTABLE_SCHEM"),
            rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME"),
            rs.getStringOption("FKTABLE_CAT"), rs.getStringOption("FKTABLE_SCHEM"),
            rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
            rs.getInt("KEY_SEQ"),
            translatePolicy(rs.getInt("UPDATE_RULE")), translatePolicy(rs.getInt("DELETE_RULE")),
            rs.getStringOption("FK_NAME"), rs.getStringOption("PK_NAME"),
            translateDeferrability(rs.getInt("DEFERRABILITY")))
    }
    
}

abstract class DbMetaDaoTests(ds: LiteDataSource) extends org.specs.Specification {
    // XXX: write some tests
    
    val jt = new JdbcTemplate(ds)
    
    // dummy
    "SELECT 1" in {
        jt.query("SELECT 1").int() must_== 1
    }
}

object PostgresqlMetaDaoTests extends DbMetaDaoTests(vendor.postgresql.PostgresqlTestDataSourceParameters.ds)

class MetaDaoTests(testsSelector: TestsSelector) extends org.specs.Specification {
    if (testsSelector.includeMysql) include(MysqlMetaDaoTests)
    if (testsSelector.includePostgresql) include(PostgresqlMetaDaoTests)
}

object MetaDaoTests extends MetaDaoTests(AllTestsSelector)

// vim: set ts=4 sw=4 et:
