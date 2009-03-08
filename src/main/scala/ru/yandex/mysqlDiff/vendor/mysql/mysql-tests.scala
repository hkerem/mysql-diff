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
        val t2 = checkTwoTables(
            "CREATE TABLE a (kk INT)",
            "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, kk INT)")
        // extra checks
        t2.primaryKey.get.columns must beLike { case Seq("id") => true }
        t2.column("id").properties.find(AutoIncrementPropertyType) must_== Some(AutoIncrement(true))
    }
    
    "diff unspecified default to script with default 0" in {
        val t2 = checkTwoTables(
            "CREATE TABLE b (x INT NOT NULL)",
            "CREATE TABLE b (x INT NOT NULL DEFAULT 0)")
        t2.column("x").properties.find(DefaultValuePropertyType) must_== Some(DefaultValue(NumberValue(0)))
    }
    
    "foreign keys with full params specification" in {
        execute("DROP TABLE IF EXISTS servers")
        execute("DROP TABLE IF EXISTS datacenters")
        execute("CREATE TABLE datacenters (id INT PRIMARY KEY) ENGINE=InnoDB")
        val t2 = checkTwoTables(
            "CREATE TABLE servers (id INT, dc_id INT) ENGINE=InnoDB",
            "CREATE TABLE servers (id INT, dc_id INT, " +
                    "CONSTRAINT dc_fk FOREIGN KEY dc_idx (dc_id) REFERENCES datacenters(id)) ENGINE=InnoDB")
        t2.foreignKeys must haveSize(1)
    }
    
    "diff, apply collate" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS collate_test")
        jdbcTemplate.execute("CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp1251_bin")
        val oldModel = jdbcModelExtractor.extractTable("collate_test", ds)
        
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
        
        val resultModel = jdbcModelExtractor.extractTable("collate_test", ds)
        // checking patch properly applied
        resultModel.options.properties must contain(MysqlCollateTableOption("cp866_bin"))
    }
    
    "same engine" in {
        checkTable(
            "CREATE TABLE yaru_events (user_id BIGINT) ENGINE=InnoDB")
    }
    
    "change engine" in {
        jdbcTemplate.execute("DROP TABLE IF exists change_engine")
        jdbcTemplate.execute("CREATE TABLE change_engine (id INT) ENGINE=MyISAM")
        val d = jdbcModelExtractor.extractTable("change_engine", ds)
        d.options.properties must contain(MysqlEngineTableOption("MyISAM"))
        val t = modelParser.parseCreateTableScript("CREATE TABLE change_engine (id INT) ENGINE=InnoDB")
        val script = new Script(diffMaker.compareTablesScript(d, t))
        script.statements must notBeEmpty
        for (s <- script.statements) {
            jdbcTemplate.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = jdbcModelExtractor.extractTable("change_engine", ds)
        resultModel.options.properties must contain(MysqlEngineTableOption("InnoDB"))
    }
    
    "bug with character set implies collation" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS moderated_tags")
        jdbcTemplate.execute("CREATE TABLE moderated_tags (tag VARCHAR(255) CHARACTER SET utf8 NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
        val s2 = "CREATE TABLE moderated_tags (tag VARCHAR(255) NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
        
        val dbModel = jdbcModelExtractor.extractTable("moderated_tags", ds)
        val localModel = modelParser.parseCreateTableScript(s2)
        
        // column collation is changed from utf8_bin to utf8_general_ci
        // (character set implies collation)
        diffMaker.compareTables(dbModel, localModel) must beLike { case Some(_) => true }
    }
    
    "bug with data type options equivalence" in {
        checkTable(
            "CREATE TABLE alive_apps (hostname VARCHAR(100) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8")
    }
    
    "bug with COLLATE before CHARACTER SET" in {
        checkTable(
            "CREATE TABLE IF NOT EXISTS tag_cloud_global (tag varchar(64) collate utf8_bin) DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
    }
    
    "BOOLEAN with DEFAULT TRUE" in {
        checkTable(
            "CREATE TABLE boolean_with_default_true (available BOOLEAN NOT NULL default TRUE)")
    }
}

// vim: set ts=4 sw=4 et:
