package ru.yandex.mysqlDiff
package vendor

import ru.yandex.small.jdbc._

import util._
import jdbc._
import model._

import Implicits._

trait TestDataSourceParameters {
    def defaultTestDsUrl: String
    val dbName =
        this.getClass.getName
            .replaceFirst("^ru.yandex.mysqlDiff.vendor.", "")
            .replaceFirst("\\..*", "")
            .ensuring { _.matches("\\w+") }
    val testDsUrlProperty = dbName + ".test.ds.url"
    
    val testDsUrl = System.getProperty(testDsUrlProperty) match {
        case null | "" => defaultTestDsUrl
        case x => x
    }
    def testDsUser: String
    def testDsPassword: String
    
    val ds = LiteDataSource.driverManager(testDsUrl, testDsUser, testDsPassword)
    
    val jdbcTemplate = new JdbcTemplate(ds)
    
    val connectedContext: ConnectedContext
}

/**
 * Base for online tests
 */
abstract class OnlineTestsSupport(val connectedContext: ConnectedContext)
    extends MySpecification with diff.DiffMakerMatchers
{
    import connectedContext._
    
    val context = connectedContext.context
    import context._
    
    private def checkTwoSimilarTableModels(a: TableModel, b: TableModel) = {
        diffMaker.compareTables(a, a) must beLike { case None => true }
        diffMaker.compareTables(b, b) must beLike { case None => true }
        diffMaker.compareTables(a, b) must beLike { case None => true }
        diffMaker.compareTables(b, a) must beLike { case None => true }
    }
    
    private def checkTwoSimilarDatabaseModels(a: DatabaseModel, b: DatabaseModel) = {
        diffMaker.compareDatabases(a, a).declDiff must beEmpty
        diffMaker.compareDatabases(b, b).declDiff must beEmpty
        diffMaker.compareDatabases(a, b).declDiff must beEmpty
        diffMaker.compareDatabases(b, a).declDiff must beEmpty
    }
    
    
    val printExecutedStmtsVar = new scala.util.DynamicVariable(false)
    def printExecutedStmts = printExecutedStmtsVar.value
    
    protected def execute(q: String) = {
        val cc = new CacheConnectionLiteDataSource(dsVar.value)
        try {
            dsVar.withValue(cc) {
                if (scriptPreamble.length > 0) {
                    if (printExecutedStmts) println(scriptPreamble)
                    ddlTemplate.executeScript(scriptPreamble)
                }
                if (printExecutedStmts) println(q)
                ddlTemplate.executeScript(q)
            }
        } finally {
            cc.close()
        }
    }
    
    protected def dropTable(tableName: String) {
        execute("DROP TABLE IF EXISTS " + tableName)
    }
    
    protected def recreateTable(script: String) {
        val cc = new CacheConnectionLiteDataSource(dsVar.value)
        try {
            dsVar.withValue(cc) {
                if (scriptPreamble.length > 0) {
                    if (printExecutedStmts) println(scriptPreamble)
                    ddlTemplate.executeScript(scriptPreamble)
                }
                if (printExecutedStmts) println("recreate " + script)
                ddlTemplate.recreateTable(script)
            }
        } finally {
            cc.close()
        }
    }

    protected def scriptPreamble = ""
    
    protected def scriptPreambleEvaluation =
        modelParser.eval(parser.parse(scriptPreamble), modelParser.emptyScriptEvaluation)
    
    def parseDb(script: String) =
        modelParser.eval(parser.parse(script), scriptPreambleEvaluation).db
    
    def parseTable(script: String) =
        parseDb(script).singleTable
    
    /**
     * Perform various tests on CREATE TABLE script.
     *
     * @param script contains table creation script
     */
    protected def checkTable(script: String) = {
        // parse model
        val t = parseTable(script)
        
        {
            if (printExecutedStmts) println("execute script, compare with parsed model")
            ddlTemplate.dropTableIfExists(t.name)
            execute(script)
            val d = jdbcModelExtractor.extractTable(t.name)
            checkTwoSimilarTableModels(t, d)
        }
        
        {
            if (printExecutedStmts) println("execute serialized parsed model, compare with parsed model")
            ddlTemplate.dropTableIfExists(t.name)
            val recreatedScript = modelSerializer.serializeTableToText(t)
            execute(recreatedScript)
            val d = jdbcModelExtractor.extractTable(t.name)
            checkTwoSimilarTableModels(t, d)
        }
        
    }
    
    private def dropTables(m: DatabaseModel) =
        for (t <- m.tables)
            ddlTemplate.dropTableWithExportedKeysIfExists(t.name)
    
    def checkDatabase(script: String) = {
        val m = parseDb(script)
        
        {
            // execute script, compare with parsed model
            dropTables(m)
            execute(script)
            val d = new DatabaseModel(m.tables.map(_.name).map(jdbcModelExtractor.extractTable(_)))
            checkTwoSimilarDatabaseModels(m, d)
        }
        
        {
            // execute serialized parsed model, compare with parsed model
            dropTables(m)
            val recreatedScript = modelSerializer.serializeDatabaseToText(m)
            execute(recreatedScript)
            val d = new DatabaseModel(m.tables.map(_.name).map(jdbcModelExtractor.extractTable(_)))
            checkTwoSimilarDatabaseModels(m, d)
        }
    }
    
    /**
     * Perform various tests comparing two <b>different</b> tables
     * @return database model of the second table after applying patch from first to second
     */
    protected def checkTwoTables(script1: String, script2: String) = {
        if (printExecutedStmts) println("checking first script equal to itself")
        checkTable(script1)
        if (printExecutedStmts) println("checking second script equal to itself")
        checkTable(script2)
        
        // perform asymmetric table comparison, treat script1 as source and script2 as target
        def checkTwoTables12(script1: String, script2: String) = {
            val t1 = parseTable(script1)
            val t2 = parseTable(script2)
            
            // tables are required to be different
            diffMaker.compareTables(t1, t2) must beLike { case Some(_) => true }
            diffMaker.compareTables(t2, t1) must beLike { case Some(_) => true }
            
            ddlTemplate.dropTableIfExists(t1.name)
            ddlTemplate.dropTableIfExists(t2.name)
            execute(script1)
            val d1 = jdbcModelExtractor.extractTable(t1.name)
            
            checkTwoSimilarTableModels(t1, d1)
            
            // compare and then apply difference
            val diff = diffMaker.compareTables(d1, t2)
            diff must beSomething
            if (printExecutedStmts) println("d1 -> t2: " + diff.get)
            for (st <- diffSerializer.serializeChangeTableDiff(diff.get, t2).ddlStatements) {
                execute(scriptSerializer.serialize(st))
            }
            val d2 = jdbcModelExtractor.extractTable(t2.name)
            
            // check result
            checkTwoSimilarTableModels(t2, d2)
            
            d2
        }
        
        if (printExecutedStmts) println("checking second to first")
        checkTwoTables12(script2, script1)
        if (printExecutedStmts) println("checking first to second")
        checkTwoTables12(script1, script2)
    }
    
    def checkTwoDatabases(script1: String, script2: String) = {
        checkDatabase(script1)
        checkDatabase(script2)
        // XXX: check transitions
        
        def checkTwoDatabases12(script1: String, script2: String) = {
            val t1 = parseDb(script1)
            val t2 = parseDb(script2)
            
            diffMaker.compareDatabases(t1, t2).declDiff must notBeEmpty
            diffMaker.compareDatabases(t2, t1).declDiff must notBeEmpty
            
            dropTables(t1)
            dropTables(t2)
            
            execute(script1)
            
            val d1 = new DatabaseModel(t1.tables.map(_.name).map(jdbcModelExtractor.extractTable(_)))
            // compare and then apply difference
            val diff = diffMaker.compareDatabases(d1, t2)
            diff.declDiff must notBeEmpty
            for (st <- diffSerializer.serializeToScript(diff, d1, t2).ddlStatements) {
                execute(scriptSerializer.serialize(st))
            }
            val d2 = new DatabaseModel(t2.tables.map(_.name).map(jdbcModelExtractor.extractTable(_)))
            
            // check result
            checkTwoSimilarDatabaseModels(t2, d2)
            
            d2
        }
        
        checkTwoDatabases12(script2, script1)
        checkTwoDatabases12(script1, script2)
    }
    
    // XXX: move tests outside
    
    "identical, simple table" in {
        checkTable("CREATE TABLE c (idc INT NOT NULL)")
    }
    
    "bug with NULL PRIMARY KEY" in {
        checkTable("CREATE TABLE null_pk (id INT NULL DEFAULT NULL, PRIMARY KEY(id))")
    }
   
    "inline UNIQUE" in {
        checkTwoTables(
            """
            CREATE TABLE trees (kind VARCHAR(100))
            """,
            """
            CREATE TABLE trees (kind VARCHAR(100) UNIQUE)
            """
        )
    }
    
    /* XXX: need drop table
    "can create table with name LIKE" in {
        val t = new TableModel("like", Seq(new ColumnModel("no_other", dataTypes.int, Seq())), Seq(), Seq())
        execute(modelSerializer.serializeTableToText(t))
        val d = jdbcModelExtractor.extractTable("like")
        checkTwoSimilarTableModels(t, d)
    }
    */

}

// vim: set ts=4 sw=4 et:
