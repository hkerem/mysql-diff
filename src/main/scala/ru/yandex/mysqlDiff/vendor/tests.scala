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
    extends org.specs.Specification
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
}

// vim: set ts=4 sw=4 et:
