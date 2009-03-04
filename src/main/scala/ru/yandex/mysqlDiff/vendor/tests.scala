package ru.yandex.mysqlDiff.vendor

import util._
import jdbc._

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

abstract class OnlineTestsSupport(val context: Context, val tdsp: TestDataSourceParameters)
    extends org.specs.Specification
{
    import context._
    import tdsp._
    
    /**
     * Execute script in database and execute diff between database model and model from script.
     * Check that both diffs are empty.
     *
     * @param script contains table creation script
     */
    protected def checkTableGeneratesNoDiff(script: String) = {
        val t = modelParser.parseCreateTableScript(script)
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + t.name)
        jdbcTemplate.execute(script)
        val d = jdbcModelExtractor.extractTable(t.name, ds)
        diffMaker.compareTables(d, t) must beLike { case None => true }
        diffMaker.compareTables(t, d) must beLike { case None => true }
    }
}

// vim: set ts=4 sw=4 et:
