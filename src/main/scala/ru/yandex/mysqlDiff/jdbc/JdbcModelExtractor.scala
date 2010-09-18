package ru.yandex.mysqlDiff
package jdbc

import java.sql._

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
    def urlDbIsCatalog = true
    
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
        
        def extractTable(tableName: String): TableModel = try {
            require(tableName.length > 0)

            val columnsList = extractTableColumns(tableName)
            
            val pk = getPrimaryKey(tableName)
            
            def columnExistsInPk(name: String) =
                pk.exists(_.columns.exists(_ == name))
            
            val fks = getFks(tableName)
            
            // MySQL adds PK to indexes, so exclude
            val indexes = getIndexes(tableName)
                    .filter(pk.isEmpty || _.columns.map(_.name).toList != pk.get.columns.map(_.name).toList)
            
            modelParser.fixTable(
                new TableModel(tableName, columnsList.toList, indexes ++ pk ++ fks, getTableOptions(tableName)))
        } catch {
            case e: Exception =>
                throw new MysqlDiffException("failed to extract table "+ tableName +" from DB: "+ e, e)
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

        def extractWithTableNamesMatching(tableNameP: String => Boolean): DatabaseModel =
            new DatabaseModel(extractTablesWithNamesMatching(tableNameP) ++ extractSequences())
        
        private val cachedTableNames = new Lazy(metaDao.findTableNames(currentCatalog, currentSchema))
        def tableNames = cachedTableNames.get
        
        private val cachedSequenceNames = new Lazy(metaDao.findSequenceNames(currentCatalog, currentSchema))
        def sequenceNames = cachedSequenceNames.get
        
        private val cachedTablesOptions = new Lazy(metaDao.findTablesOptions(currentCatalog, currentSchema))
        
        def getTableOptions(tableName: String): Seq[TableOption] =
            cachedTablesOptions.get.find(_._1 == tableName).get._2
        
        def extractTablesWithNamesMatching(tableNameP: String => Boolean): Seq[TableModel] = {
            tableNames.filter(tableNameP).map(extractTable _)
        }

        def extractTables(): Seq[TableModel] = {
            val trueP = (name: String) => true
            extractTablesWithNamesMatching(trueP)
        }
        
        def extractSequences(): Seq[SequenceModel] = {
            sequenceNames.map(extractSequence _)
        }
    }
    
    protected def useParserToParseDefaultValue = true
    
    /** Convert value from DB to specified database type */
    protected def parseDefaultValueFromDb(s: String, dataType: DataType): Option[script.SqlExpr] = {
        def parse0() = {
            // XXX: move all this stuff to parser combinator
            if (dataTypes.isAnyChar(dataType.name)) {
                if (s matches "'.*'") Some(StringValue(s.replaceFirst("^'", "").replaceFirst("'$", "")))
                else Some(StringValue(s))
            }
            else if (s == "NULL") None
            else if (dataTypes.isAnyDateTime(dataType.name)) {
                if (s == "CURRENT_TIMESTAMP") Some(NowValue)
                else Some(StringValue(s))
            }
            else if (dataTypes.isAnyNumber(dataType.name)) {
                s match {
                    case "" | "\0" => Some(NumberValue(0))
                    case _ => Some(sqlParserCombinator.parseValue(s))
                }
            }
            else Some(StringValue(s))
        }
        
        if (s == null) Some(NullValue) // None
        else {
            if (useParserToParseDefaultValue) {
                try {
                    Some(parser.parseSqlExpr(s))
                } catch {
                    // XXX: catch specific exceptions
                    case _: Exception => parse0()
                }
            } else {
                parse0()
            }
        }
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
    
    def extractWithTableNamesMatching(tableNameP: String => Boolean): DatabaseModel =
        newAllTablesSchemaExtractor().extractWithTableNamesMatching(tableNameP)
}

class JdbcModelExtractorTests(connectedContext: ConnectedContext)
    extends MySpecification
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

    import ddlTemplate._

    "ignore tables when extracting" in {
        recreateTable("CREATE TABLE a (aa INT)")
        recreateTable("CREATE TABLE b (bb INT)")
        recreateTable("CREATE TABLE c (cc INT)")
        val tableNames = jdbcModelExtractor.extractWithTableNamesMatching(_ != "c").tables.map(_.name)
        tableNames.contains("c") must_== false
        tableNames.contains("a") must_== true
        tableNames.contains("b") must_== true
    }
}


// vim: set ts=4 sw=4 et:
