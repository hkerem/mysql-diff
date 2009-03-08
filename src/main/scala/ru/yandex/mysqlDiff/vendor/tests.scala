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
}

/**
 * Base for online tests
 */
abstract class OnlineTestsSupport(val context: Context, val tdsp: TestDataSourceParameters)
    extends org.specs.Specification with diff.DiffMakerMatchers
{
    import context._
    import tdsp._
    
    private def checkTwoSimilarTableModels(a: TableModel, b: TableModel) = {
        diffMaker.compareTables(a, b) must beLike { case None => true }
        diffMaker.compareTables(b, a) must beLike { case None => true }
        diffMaker.compareTables(a, a) must beLike { case None => true }
        diffMaker.compareTables(b, b) must beLike { case None => true }
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
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + t.name)
            jdbcTemplate.execute(script)
            val d = jdbcModelExtractor.extractTable(t.name, ds)
            checkTwoSimilarTableModels(t, d)
        }
        
        {
            // execute serialized parsed model, compare with parsed model
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + t.name)
            val recreatedScript = modelSerializer.serializeTableToText(t)
            jdbcTemplate.execute(recreatedScript)
            val d = jdbcModelExtractor.extractTable(t.name, ds)
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
            
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + t1.name)
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + t2.name)
            jdbcTemplate.execute(script1)
            val d1 = jdbcModelExtractor.extractTable(t1.name, ds)
            
            checkTwoSimilarTableModels(t1, d1)
            
            // compare and then apply difference
            val diff = diffMaker.compareTables(d1, t2)
            diff must beSomething
            for (st <- diffSerializer.serializeChangeTableDiff(diff.get, t2).ddlStatements) {
                jdbcTemplate.execute(scriptSerializer.serialize(st))
            }
            val d2 = jdbcModelExtractor.extractTable(t2.name, ds)
            
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

}

// vim: set ts=4 sw=4 et:
