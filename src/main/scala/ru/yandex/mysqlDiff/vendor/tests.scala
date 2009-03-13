package ru.yandex.mysqlDiff.vendor

import util._
import jdbc._
import model._

import Implicits._

trait TestDataSourceParameters {
    val defaultTestDsUrl: String
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
    val testDsUser: String
    val testDsPassword: String
    
    val ds = LiteDataSource.driverManager(testDsUrl, testDsUser, testDsPassword)
    
    val jdbcTemplate = new util.JdbcTemplate(ds)
    
    val connectedContext: ConnectedContext
}

/**
 * Base for online tests
 */
abstract class OnlineTestsSupport(val connectedContext: ConnectedContext)
    extends org.specs.Specification with diff.DiffMakerMatchers
{
    import connectedContext._
    
    val context = connectedContext.context
    import context._
    
    private def checkTwoSimilarTableModels(a: TableModel, b: TableModel) = {
        diffMaker.compareTables(a, b) must beLike { case None => true }
        diffMaker.compareTables(b, a) must beLike { case None => true }
        diffMaker.compareTables(a, a) must beLike { case None => true }
        diffMaker.compareTables(b, b) must beLike { case None => true }
    }
    
    
    val printExecutedStmts = true
    
    protected def execute(q: String) = {
        if (printExecutedStmts) println(q)
        jt.execute(q)
    }
    
    
    /**
     * Perform various tests on CREATE TABLE script.
     *
     * @param script contains table creation script
     */
    protected def checkTable(script: String) = {
        // parse model
        val t = modelParser.parseCreateTableScript(script)
        
        {
            // execute script, compare with parsed model
            ddlTemplate.dropTableIfExists(t.name)
            execute(script)
            val d = jdbcModelExtractor.extractTable(t.name)
            checkTwoSimilarTableModels(t, d)
        }
        
        {
            // execute serialized parsed model, compare with parsed model
            ddlTemplate.dropTableIfExists(t.name)
            val recreatedScript = modelSerializer.serializeTableToText(t)
            execute(recreatedScript)
            val d = jdbcModelExtractor.extractTable(t.name)
            checkTwoSimilarTableModels(t, d)
        }
        
    }
    
    /**
     * Perform various tests comparing two <b>different</b> tables
     * @return database model of the second table after applying patch from first to second
     */
    protected def checkTwoTables(script1: String, script2: String) = {
        checkTable(script1)
        checkTable(script2)
        
        // perform asymmetric table comparison, treat script1 as source and script2 as target
        def checkTwoTables12(script1: String, script2: String) = {
            val t1 = modelParser.parseCreateTableScript(script1)
            val t2 = modelParser.parseCreateTableScript(script2)
            
            // tables are required to be different
            diffMaker.compareTables(t1, t2) must beLike { case Some(_) => true }
            
            ddlTemplate.dropTableIfExists(t1.name)
            ddlTemplate.dropTableIfExists(t2.name)
            execute(script1)
            val d1 = jdbcModelExtractor.extractTable(t1.name)
            
            checkTwoSimilarTableModels(t1, d1)
            
            // compare and then apply difference
            val diff = diffMaker.compareTables(d1, t2)
            diff must beSomething
            for (st <- diffSerializer.serializeChangeTableDiff(diff.get, t2).ddlStatements) {
                execute(scriptSerializer.serialize(st))
            }
            val d2 = jdbcModelExtractor.extractTable(t2.name)
            
            // check result
            checkTwoSimilarTableModels(t2, d2)
            
            d2
        }
        
        checkTwoTables12(script2, script1)
        checkTwoTables12(script1, script2)
    }
    
    "identical, simple table" in {
        checkTable("CREATE TABLE c (idc INT NOT NULL)")
    }
    
    "bug with NULL PRIMARY KEY" in {
        checkTable("CREATE TABLE null_pk (id INT NULL DEFAULT NULL, PRIMARY KEY(id))")
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
