package ru.yandex.mysqlDiff.jdbc

import java.sql._
import util._

import model._

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

import vendor.mysql._
    
/**
 * DAO to query database information.
 */
class MetaDao(jt: JdbcTemplate) {
    def this(ds: () => Connection) = this(new JdbcTemplate(ds))
    def this(ds: LiteDataSource) = this(new JdbcTemplate(ds))
    
    import jt._
    import MetaDao.read
    
    
    def findPrimaryKey(catalog: String, schema: String, tableName: String): Option[PrimaryKeyModel] =
        execute { conn =>
            val rs = conn.getMetaData.getPrimaryKeys(catalog, schema, tableName)

            case class R(pkName: String, columnName: String, keySeq: Int)

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

                Some(new PrimaryKeyModel(pkNameO, new IndexModel(None, r.map(_.columnName))))
            }
        }
    
    /**
     * Find all tables in specified catalog and schema.
     */
    def findTableNames(catalog: String, schema: String) = execute { conn =>
        val data = conn.getMetaData
        
        val rs = data.getTables(catalog, schema, "%", List("TABLE").toArray)
        
        read(rs) { rs =>
            rs.getString("TABLE_NAME")
        }
    }
    
    // regular indexes
    def findIndexes(catalog: String, schema: String, tableName: String): Seq[HasIndexModel] = execute { conn =>
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
            
            val index = new IndexModel(Some(indexName), rows.map(_.columnName))
            
            if (unique) new UniqueKeyModel(None, index)
            else index
        }
    }
    
    def findImportedKeys(catalog: String, schema: String, tableName: String): Seq[ForeignKeyModel] =
        execute { conn =>
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
                val index = new IndexModel(Some(keyName), rows.map(_.localColumnName))
                ForeignKeyModel(Some(keyName), index,
                        externalTableNames.elements.next, rows.map(_.externalColumnName))
            }
        }
   
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
        return Seq()
    }
    

}

object MetaDao {

    /**
     * Utility to read rows from ResultSet.
     */
    def read[T](rs: ResultSet)(f: ResultSet => T) = {
        val r = new ArrayBuffer[T]()
        while (rs.next()) {
            r += f(rs)
        }
        r
    }
    
}

abstract class DbMetaDaoTests(ds: LiteDataSource) extends org.specs.Specification {
    // XXX: write some tests
    
    val jt = new util.JdbcTemplate(ds)
    
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
