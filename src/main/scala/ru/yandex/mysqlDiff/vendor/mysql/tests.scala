package ru.yandex.mysqlDiff.vendor.mysql

import java.sql._

import jdbc._
import diff._
import model._
import script._
import util._

object MysqlTestDataSourceParameters extends TestDataSourceParameters {
    
    override val testDsUrl = "jdbc:mysql://fastshot:3306/mysql_diff_test"
    override val testDsUser = "test"
    override val testDsPassword = "test"
    
}

object MysqlOnlineTests extends org.specs.Specification {
    import MysqlTestDataSourceParameters._
    import Environment.defaultContext._
    
    "CAP-101" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS a")
        
        jdbcTemplate.execute("CREATE TABLE a (kk INT)")
        val nk = "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, kk INT)"
        val oldModel = JdbcModelExtractor.extractTable("a", ds)
        val newModel = modelParser.parseCreateTableScript(nk)
        val diff = diffMaker.compareTables(oldModel, newModel).get
        val script = new Script(TableScriptBuilder.alterScript(diff, newModel)).statements
        //script.foreach((s: ScriptStatement) => println(s.serialize))
        script.foreach((s: ScriptStatement) => jdbcTemplate.execute(s.serialize))
        val gotModel = JdbcModelExtractor.extractTable("a", ds)
        // XXX: check model
        ()
    }
    
    "diff unspecified default to script with default 0" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS b")
        
        jdbcTemplate.execute("CREATE TABLE b (x INT NOT NULL)")
        val oldModel = JdbcModelExtractor.extractTable("b", ds)
        
        val newModel = modelParser.parseCreateTableScript("CREATE TABLE b (x INT NOT NULL DEFAULT 0)")
        
        val diff = diffMaker.compareTables(oldModel, newModel).get
        
        val ChangeTableDiff("b", None, Seq(columnDiff), Seq()) = diff
        val ChangeColumnDiff("x", None, Seq(propertyDiff)) = columnDiff
        propertyDiff match { // must be any of
            case ChangeColumnPropertyDiff(oldP, newP) =>
            case CreateColumnPropertyDiff(p) =>
        }
    }
    
    "identical" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS c")
        
        jdbcTemplate.execute("CREATE TABLE c (idc BIGINT NOT NULL)")
        val oldModel = JdbcModelExtractor.extractTable("c", ds)
        
        val newModel = modelParser.parseCreateTableScript("CREATE TABLE c (idc BIGINT NOT NULL)")
        
        diffMaker.compareTables(oldModel, newModel) must_== None
    }
    
}

// vim: set ts=4 sw=4 et:
