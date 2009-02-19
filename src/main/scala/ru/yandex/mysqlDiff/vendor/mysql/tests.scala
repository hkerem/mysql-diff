package ru.yandex.mysqlDiff.vendor.mysql

import java.sql._

import jdbc._
import diff._
import model._
import script._
import util._

object MysqlTestDataSourceParameters extends TestDataSourceParameters {
    
    override val defaultTestDsUrl = "jdbc:mysql://localhost:3306/mysql_diff_test"
    override val testDsUser = "test"
    override val testDsPassword = "test"
    
}

object MysqlOnlineTests extends org.specs.Specification {
    import MysqlTestDataSourceParameters._
    // XXX: use proper context
    import Environment.defaultContext._
    
    "CAP-101" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS a")
        
        jdbcTemplate.execute("CREATE TABLE a (kk INT)")
        val nk = "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, kk INT)"
        val oldModel = JdbcModelExtractor.extractTable("a", ds)
        val newModel = modelParser.parseCreateTableScript(nk)
        val diff = diffMaker.compareTables(oldModel, newModel).get
        val script = new Script(diffSerializer.alterScript(diff, newModel)).statements
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
        
        val ChangeTableDiff("b", None, Seq(columnDiff), Seq(), Seq()) = diff
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
    
    "diff, apply collate" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS collate_test")
        jdbcTemplate.execute("CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp1251_bin")
        val oldModel = JdbcModelExtractor.extractTable("collate_test", ds)
        
        // checking properly extracted
        oldModel.options must contain(TableOption("COLLATE", "cp1251_bin"))
        
        val newModel = modelParser.parseCreateTableScript(
            "CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp866_bin")
        // checking properly parsed
        newModel.options must contain(TableOption("COLLATE", "cp866_bin"))
        
        val diff = diffMaker.compareTables(oldModel, newModel)
        diff must beSomething
        
        val script = new Script(diffSerializer.alterScript(diff.get, newModel))
        
        script.statements must haveSize(1)
        
        for (s <- script.statements) {
            jdbcTemplate.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = JdbcModelExtractor.extractTable("collate_test", ds)
        // checking patch properly applied
        resultModel.options must contain(TableOption("COLLATE", "cp866_bin"))
    }
    
    "same engine" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS yaru_events")
        val s = "CREATE TABLE yaru_events (user_id BIGINT) ENGINE=InnoDB"
        jdbcTemplate.execute(s)
        val d = JdbcModelExtractor.extractTable("yaru_events", ds)
        val t = modelParser.parseCreateTableScript(s)
        diffMaker.compareTables(d, t) must beEmpty
    }
    
    "change engine" in {
        jdbcTemplate.execute("DROP TABLE IF exists change_engine")
        jdbcTemplate.execute("CREATE TABLE change_engine (id INT) ENGINE=MyISAM")
        val d = JdbcModelExtractor.extractTable("change_engine", ds)
        d.options must contain(TableOption("ENGINE", "MyISAM"))
        val t = modelParser.parseCreateTableScript("CREATE TABLE change_engine (id INT) ENGINE=InnoDB")
        val script = new Script(diffMaker.compareTablesScript(d, t))
        script.statements must notBeEmpty
        for (s <- script.statements) {
            jdbcTemplate.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = JdbcModelExtractor.extractTable("change_engine", ds)
        resultModel.options must contain(TableOption("ENGINE", "InnoDB"))
    }
}

// vim: set ts=4 sw=4 et:
