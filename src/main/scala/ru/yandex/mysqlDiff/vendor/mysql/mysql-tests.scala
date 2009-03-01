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

object MysqlOnlineTests extends OnlineTestsSupport(MysqlContext, MysqlTestDataSourceParameters) {
    import tdsp._
    import context._
    
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
        oldModel.options.properties must contain(MysqlCollateTableOption("cp1251_bin"))
        
        val newModel = modelParser.parseCreateTableScript(
            "CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp866_bin")
        // checking properly parsed
        newModel.options.properties must contain(MysqlCollateTableOption("cp866_bin"))
        
        val diff = diffMaker.compareTables(oldModel, newModel)
        diff must beSomething
        
        val script = new Script(diffSerializer.alterScript(diff.get, newModel))
        
        script.statements must haveSize(1)
        
        for (s <- script.statements) {
            jdbcTemplate.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = JdbcModelExtractor.extractTable("collate_test", ds)
        // checking patch properly applied
        resultModel.options.properties must contain(MysqlCollateTableOption("cp866_bin"))
    }
    
    "same engine" in {
        checkTableGeneratesNoDiff(
            "CREATE TABLE yaru_events (user_id BIGINT) ENGINE=InnoDB")
    }
    
    "change engine" in {
        jdbcTemplate.execute("DROP TABLE IF exists change_engine")
        jdbcTemplate.execute("CREATE TABLE change_engine (id INT) ENGINE=MyISAM")
        val d = JdbcModelExtractor.extractTable("change_engine", ds)
        d.options.properties must contain(MysqlEngineTableOption("MyISAM"))
        val t = modelParser.parseCreateTableScript("CREATE TABLE change_engine (id INT) ENGINE=InnoDB")
        val script = new Script(diffMaker.compareTablesScript(d, t))
        script.statements must notBeEmpty
        for (s <- script.statements) {
            jdbcTemplate.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = JdbcModelExtractor.extractTable("change_engine", ds)
        resultModel.options.properties must contain(MysqlEngineTableOption("InnoDB"))
    }
    
    "bug with NULL PRIMARY KEY" in {
        checkTableGeneratesNoDiff(
            "CREATE TABLE null_pk (id INT NULL DEFAULT NULL, PRIMARY KEY(id))")
    }
    
    "bug with character set implies collation" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS moderated_tags")
        jdbcTemplate.execute("CREATE TABLE moderated_tags (tag VARCHAR(255) CHARACTER SET utf8 NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
        val s2 = "CREATE TABLE moderated_tags (tag VARCHAR(255) NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
        
        val dbModel = JdbcModelExtractor.extractTable("moderated_tags", ds)
        val localModel = modelParser.parseCreateTableScript(s2)
        
        // column collation is changed from utf8_bin to utf8_general_ci
        // (character set implies collation)
        diffMaker.compareTables(dbModel, localModel) must beLike { case Some(_) => true }
    }
    
    "bug with data type options equivalence" in {
        checkTableGeneratesNoDiff(
            "CREATE TABLE alive_apps (hostname VARCHAR(100) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8")
    }
}

// vim: set ts=4 sw=4 et:
