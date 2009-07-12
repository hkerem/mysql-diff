package ru.yandex.mysqlDiff.jdbc

import java.sql._

import scalax.control.ManagedResource

import scala.util.Sorting._
import scala.collection.mutable.ArrayBuffer

import model._
import util._

import Implicits._

class JdbcModelExtractorException(msg: String, cause: Throwable) extends Exception(msg, cause)

object JdbcModelExtractor {
    def dbNameFromUrl(url: String) =
        url.replaceFirst("\\?.*", "").replaceFirst(".*[/:]", "")
            .ensuring(_.matches("(\\w|-)+"), "could not extract database name from URL: " + url)
}

/*
 * Table model from the live database.
 */
class JdbcModelExtractor(connectedContext: ConnectedContext) {
    import JdbcModelExtractor._
    import JdbcUtils._
    
    import connectedContext._
    import context._
    
    import MetaDao._

    /** Database name from JDBC URL */
    lazy val currentDb =
        dbNameFromUrl(jt.metaData(_.getURL))
    
    /**
     * True iff last part of URL denotes DB catalog (PostgreSQL),
     * and false iff denotes schema (MySQL).
     */
    val urlDbIsCatalog = false
    
    /** Value to be passed as first param to <code>DatabaseMetaData</code> methods */
    def currentCatalog: String = if (urlDbIsCatalog) currentDb else null
    /** Value to be passed as second param to <code>DatabaseMetaData</code> methods */
    def currentSchema: String = if (urlDbIsCatalog) null else currentDb
    
    /**
     * Like lazy keyword, but has isCreated method.
     */
    class Lazy[T](create: => T) {
        var value: Option[T] = None
        def get = {
            if (value.isEmpty) value = Some(create)
            value.get
        }
        /** If already initialized */
        def isCreated = value.isDefined
    }
    
    /**
     * Stateful object.
     */
    protected abstract class SchemaExtractor {
        import jt._
        
        
        /** Uses only JDBC API, can be overriden by subclasses */
        protected def parseTableColumn(columns: ResultSet) = {
            val colName = columns.getString("COLUMN_NAME")
            
            val colType = columns.getString("TYPE_NAME")
            
            // None if NULL
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
            
            // XXX: drop mysql
            val autoIncrement = columns.getString("IS_AUTOINCREMENT") match {
                case "YES" => Some(vendor.mysql.MysqlAutoIncrement(true))
                case "NO" => Some(vendor.mysql.MysqlAutoIncrement(false))
                case "" => None
            }
            
            val defaultValueFromDb =
                columns.getString("COLUMN_DEF")
            
            val dataType = dataTypes.make(colType, colTypeSize)
            
            val defaultValue =
                try {
                    parseDefaultValueFromDb(defaultValueFromDb, dataType).map(DefaultValue(_))
                } catch {
                    case e: Exception =>
                        throw new MysqlDiffException(
                            "failed to parse column \""+ colName +"\" default value: " + e, e)
                }
            
            val props = Seq[ColumnProperty]() ++ nullable ++ defaultValue ++ autoIncrement
            new ColumnModel(colName, dataType, props)
        }
        
        def extractTableColumns(tableName: String): Seq[ColumnModel] = metaData { data =>
            require(tableName.length > 0)

            val columns = data.getColumns(currentCatalog, currentSchema, tableName, "%")
            columns.read(parseTableColumn _)
        }
        
        def extractTable(tableName: String): TableModel = {
            require(tableName.length > 0)

            val columnsList = extractTableColumns(tableName)
            
            val pk = getPrimaryKey(tableName)
            
            def columnExistsInPk(name: String) =
                pk.exists(_.columns.exists(_ == name))
            
            val fks = getFks(tableName)
            
            // MySQL adds PK to indexes, so exclude
            val indexes = getIndexes(tableName)
                    .filter(pk.isEmpty || _.columns.toList != pk.get.columns.toList)
            
            modelParser.fixTable(
                new TableModel(tableName, columnsList.toList, indexes ++ pk ++ fks, getTableOptions(tableName)))
        }
        
        def extractSequence(sequenceName: String): SequenceModel = {
            new SequenceModel(sequenceName) // XXX: too lame
        }
        
        def getPrimaryKey(tableName: String): Option[PrimaryKeyModel] =
            metaDao.findPrimaryKey(currentCatalog, currentSchema, tableName)
        
        /** Indexes plus unique constraints */
        def getIndexes(tableName: String): Seq[UniqueOrIndexModel] =
            metaDao.findIndexes(currentCatalog, currentSchema, tableName)

        def getFks(tableName: String): Seq[ForeignKeyModel] =
            metaDao.findImportedKeyModels(currentCatalog, currentSchema, tableName)
        
        /** Not including PK */
        def getKeys(tableName: String): Seq[TableExtra] =
            getIndexes(tableName) ++ getFks(tableName)

        def getTableOptions(tableName: String): Seq[TableOption]
        
    }
    
    /** Schema extractor optimized for single table extraction */
    protected class SingleTableSchemaExtractor extends SchemaExtractor {
        override def getTableOptions(tableName: String) =
            metaDao.findTableOptions(currentCatalog, currentSchema, tableName)
        
    }
    
    /** Schema extractor optimized for extraction of all schema */
    protected class AllTablesSchemaExtractor extends SchemaExtractor {
    
        def extract(): DatabaseModel =
            new DatabaseModel(extractTables() ++ extractSequences())
        
        private val cachedTableNames = new Lazy(metaDao.findTableNames(currentCatalog, currentSchema))
        def tableNames = cachedTableNames.get
        
        private val cachedSequenceNames = new Lazy(metaDao.findSequenceNames(currentCatalog, currentSchema))
        def sequenceNames = cachedSequenceNames.get
        
        private val cachedTablesOptions = new Lazy(metaDao.findTablesOptions(currentCatalog, currentSchema))
        
        def getTableOptions(tableName: String): Seq[TableOption] =
            cachedTablesOptions.get.find(_._1 == tableName).get._2
        
        def extractTables(): Seq[TableModel] = {
            tableNames.map(extractTable _)
        }
        
        def extractSequences(): Seq[SequenceModel] = {
            sequenceNames.map(extractSequence _)
        }
    
    }
    
    /** Convert value from DB to specified database type */
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
            if (s == "") Some(NumberValue(0))
            else Some(sqlParserCombinator.parseValue(s))
        }
        else Some(StringValue(s))
    }
    
    
    protected def newAllTablesSchemaExtractor() =
        new AllTablesSchemaExtractor
    
    protected def newSingleTableSchemaExtractor() =
        new SingleTableSchemaExtractor
    
    protected def extractTables(): Seq[TableModel] =
        newAllTablesSchemaExtractor().extractTables()
    
    def extractTable(tableName: String): TableModel =
        newSingleTableSchemaExtractor().extractTable(tableName)
    
    def extract(): DatabaseModel =
        newAllTablesSchemaExtractor().extract()
    

}

class JdbcModelExtractorTests(connectedContext: ConnectedContext)
    extends org.specs.Specification
{
    import connectedContext._
    import connectedContext.context._
    
    import jt.execute
    
    protected def dropTable(tableName: String) {
        execute("DROP TABLE IF EXISTS " + tableName)
    }
    
    protected def extractTable(tableName: String) = {
        jdbcModelExtractor.extractTable(tableName)
    }
    
    // XXX: no need to execute in each vendor
    "dbNameFromUrl" in {
        JdbcModelExtractor.dbNameFromUrl(
            "jdbc:mysql://localhost:3306/dev-supersaveit?user=web") must_== "dev-supersaveit"
    }
}


// vim: set ts=4 sw=4 et:
