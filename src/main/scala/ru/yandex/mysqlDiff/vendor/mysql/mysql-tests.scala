package ru.yandex.mysqlDiff.vendor.mysql

import java.sql._

import jdbc._
import diff._
import model._
import script._
import util._

object MysqlTestDataSourceParameters extends TestDataSourceParameters {
    
    override val defaultTestDsUrl = "jdbc:mysql://localhost:3306/mysql_diff_tests"
    override val testDsUser = "test"
    override val testDsPassword = "test"
    
    override val connectedContext = new MysqlConnectedContext(ds)
}

object MysqlOnlineTests extends OnlineTestsSupport(MysqlTestDataSourceParameters.connectedContext) {
    import connectedContext._
    import context._
    
    "CAP-101" in {
        val t2 = checkTwoTables(
            "CREATE TABLE a (kk INT)",
            "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, kk INT)")
        // extra checks
        t2.primaryKey.get.columnNames.toList must_== List("id")
        t2.column("id").properties.find(MysqlAutoIncrementPropertyType) must_== Some(MysqlAutoIncrement(true))
    }
    
    "diff unspecified default to script with default 0" in {
        val t2 = checkTwoTables(
            "CREATE TABLE b (x INT NOT NULL)",
            "CREATE TABLE b (x INT NOT NULL DEFAULT 0)")
        t2.column("x").properties.find(DefaultValuePropertyType) must_== Some(DefaultValue(NumberValue(0)))
    }
    
    "foreign keys with full params specification" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("servers")
        ddlTemplate.dropTableWithExportedKeysIfExists("datacenters")
        execute("CREATE TABLE datacenters (id INT PRIMARY KEY) ENGINE=InnoDB")
        val t2 = checkTwoTables(
            "CREATE TABLE servers (id INT, dc_id INT) ENGINE=InnoDB",
            "CREATE TABLE servers (id INT, dc_id INT, " +
                    "CONSTRAINT dc_fk FOREIGN KEY dc_idx (dc_id) REFERENCES datacenters(id)) ENGINE=InnoDB")
        t2.foreignKeys must haveSize(1)
    }
    
    "FOREIGN KEY with overlapping INDEX" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("yyyy")
        ddlTemplate.dropTableWithExportedKeysIfExists("zzzz")
        ddlTemplate.recreateTable("CREATE TABLE zzzz (id INT PRIMARY KEY) ENGINE=InnoDB")
        checkTwoTables(
            "CREATE TABLE yyyy (id INT, zzzz_id INT, CONSTRAINT zzzz_c FOREIGN KEY zzzz_i (zzzz_id) REFERENCES zzzz(id), INDEX(zzzz_id, id)) ENGINE=InnoDB",
            "CREATE TABLE yyyy (id INT, zzzz_id INT, CONSTRAINT zzzz_c FOREIGN KEY zzzz_i (zzzz_id) REFERENCES zzzz(id)) ENGINE=InnoDB"
            )
    }
    
    "FOREIGN KEY with overlapping UNIQUE" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("uuuu")
        ddlTemplate.dropTableWithExportedKeysIfExists("qqqq")
        ddlTemplate.recreateTable("CREATE TABLE qqqq (id INT PRIMARY KEY) ENGINE=InnoDB")
        checkTwoTables(
            "CREATE TABLE uuuu (id INT, qqqq_id INT, CONSTRAINT qqqq_c FOREIGN KEY qqqq_i (qqqq_id) REFERENCES qqqq(id), UNIQUE(qqqq_id, id)) ENGINE=InnoDB",
            "CREATE TABLE uuuu (id INT, qqqq_id INT, CONSTRAINT qqqq_c FOREIGN KEY qqqq_i (qqqq_id) REFERENCES qqqq(id)) ENGINE=InnoDB"
            )
    }
    
    "FOREIGN KEY with overlapping PRIMARY KEY" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("mmmm")
        ddlTemplate.dropTableWithExportedKeysIfExists("nnnn")
        ddlTemplate.recreateTable("CREATE TABLE mmmm (id INT PRIMARY KEY) ENGINE=InnoDB")
        checkTwoTables(
            "CREATE TABLE nnnn (id INT, mmmm_id INT, CONSTRAINT mmmm_c FOREIGN KEY mmmm_i (mmmm_id) REFERENCES mmmm(id), PRIMARY KEY(mmmm_id, id)) ENGINE=InnoDB",
            "CREATE TABLE nnnn (id INT, mmmm_id INT, CONSTRAINT mmmm_c FOREIGN KEY mmmm_i (mmmm_id) REFERENCES mmmm(id)) ENGINE=InnoDB"
            )
    }
    
    "FOREIGN KEY ... ON UPDATE ... ON DELETE" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("o_u_o_d")
        ddlTemplate.dropTableWithExportedKeysIfExists("o_u_o_d_pri")
        jt.execute("CREATE TABLE o_u_o_d_pri (id INT PRIMARY KEY) ENGINE=InnoDB")
        checkTwoTables(
            "CREATE TABLE o_u_o_d (a_id INT, id INT, " +
                "FOREIGN KEY (a_id) REFERENCES o_u_o_d_pri(id) ON UPDATE SET NULL) ENGINE=InnoDB",
            "CREATE TABLE o_u_o_d (a_id INT, id INT, " +
                "FOREIGN KEY (a_id) REFERENCES o_u_o_d_pri(id) ON UPDATE CASCADE ON DELETE SET NULL) ENGINE=InnoDB"
            )
    }
    
    "diff, apply collate" in {
        jt.execute("DROP TABLE IF EXISTS collate_test")
        jt.execute("CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp1251_bin")
        val oldModel = jdbcModelExtractor.extractTable("collate_test")
        
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
            jt.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = jdbcModelExtractor.extractTable("collate_test")
        // checking patch properly applied
        resultModel.options.properties must contain(MysqlCollateTableOption("cp866_bin"))
    }
    
    "same engine" in {
        checkTable(
            "CREATE TABLE yaru_events (user_id BIGINT) ENGINE=InnoDB")
    }
    
    "change engine" in {
        jt.execute("DROP TABLE IF exists change_engine")
        jt.execute("CREATE TABLE change_engine (id INT) ENGINE=MyISAM")
        val d = jdbcModelExtractor.extractTable("change_engine")
        d.options.properties must contain(MysqlEngineTableOption("MyISAM"))
        val t = modelParser.parseCreateTableScript("CREATE TABLE change_engine (id INT) ENGINE=InnoDB")
        val script = new Script(diffMaker.compareTablesScript(d, t))
        script.statements must notBeEmpty
        for (s <- script.statements) {
            jt.execute(scriptSerializer.serialize(s))
        }
        
        val resultModel = jdbcModelExtractor.extractTable("change_engine")
        resultModel.options.properties must contain(MysqlEngineTableOption("InnoDB"))
    }
    
    "ENUM" in {
        checkTable("CREATE TABLE users (id INT, department ENUM('dev', 'mngr', 'adm') DEFAULT 'mngr')")
    }
    
    "bug with character set implies collation" in {
        jt.execute("DROP TABLE IF EXISTS moderated_tags")
        jt.execute("CREATE TABLE moderated_tags (tag VARCHAR(255) CHARACTER SET utf8 NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
        val s2 = "CREATE TABLE moderated_tags (tag VARCHAR(255) NOT NULL, type INT(11) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
        
        val dbModel = jdbcModelExtractor.extractTable("moderated_tags")
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
    
    "PRIMARY KEY not declared as NOT NULL" in {
        checkTable(
            "CREATE TABLE pk_null (id VARCHAR(10), PRIMARY KEY(id))")
    }
    
    "BOOLEAN with DEFAULT TRUE" in {
        checkTable(
            "CREATE TABLE boolean_with_default_true (available BOOLEAN NOT NULL default TRUE)")
    }
    
    "BOOLEAN with DEFAULT 1" in {
        checkTable(
            "CREATE TABLE boolean_with_default_true (available BOOLEAN NOT NULL default 1)")
    }
    
    "DECIMAL DEFAULT VALUE" in {
        checkTable("CREATE TABLE decimal_dv (id INT, deci DECIMAL(10, 6) DEFAULT '0.000000')")
    }
    
    "MEDIUMINT UNSIGNED" in {
        checkTable(
            "CREATE TABLE service_with_mediumint_unsigned (bg_color MEDIUMINT UNSIGNED)")
    }
    
    "bug with PRIMARY KEY added" in {
        checkTwoTables(
            "CREATE TABLE im_cs1 (checksum VARCHAR(255), added INT, PRIMARY KEY(checksum), KEY(added))",
            "CREATE TABLE im_cs1 (checksum VARCHAR(255), added INT, KEY(added))")
    }
    
    "bug with two PRIMARY KEYs added" in {
        checkTwoTables(
            "CREATE TABLE im_cs (c1 INT, c2 INT, added INT, PRIMARY KEY(c1, c2))",
            "CREATE TABLE im_cs (c1 INT, c2 INT, added INT)")
    }
    
    "bug with non-unique CONSTRAINT names" in {
        checkTwoDatabases(
            """
            CREATE TABLE nu_a (id INT PRIMARY KEY) ENGINE=InnoDB;
            CREATE TABLE nu_b (a_id INT) ENGINE=InnoDB;
            CREATE TABLE nu_c (a_id INT, CONSTRAINT c866 FOREIGN KEY zc (a_id) REFERENCES nu_a(id)) ENGINE=InnoDB
            """,
            """
            CREATE TABLE nu_a (id INT PRIMARY KEY) ENGINE=InnoDB;
            CREATE TABLE nu_b (a_id INT, CONSTRAINT c866 FOREIGN KEY zb (a_id) REFERENCES nu_a(id)) ENGINE=InnoDB;
            CREATE TABLE nu_c (a_id INT) ENGINE=InnoDB
            """)
    }
    
    "bug with ADD FOREIGN KEY referenced to newly created table" in {
        checkTwoDatabases(
            """
            CREATE TABLE cities (id INT PRIMARY KEY) ENGINE=InnoDB
            """,
            """
            CREATE TABLE countries (id INT PRIMARY KEY) ENGINE=InnoDB;
            CREATE TABLE cities (id INT PRIMARY KEY, cid INT, CONSTRAINT ccid FOREIGN KEY icid (cid) REFERENCES countries(id))
                    ENGINE=InnoDB
            """)
    }
    
}

object MysqlDdlTemplateTests extends DdlTemplateTests(MysqlTestDataSourceParameters.connectedContext) {
    import MysqlTestDataSourceParameters.connectedContext._
    import MysqlTestDataSourceParameters.connectedContext.context._
    
    import ddlTemplate._
    import jt.execute
    
    "dropTableWithExportedKeys" in {
        dropTableWithExportedKeysIfExists("dt_a")
        dropTableWithExportedKeysIfExists("dt_b")
        
        execute("CREATE TABLE dt_a (id INT PRIMARY KEY, b_id INT) ENGINE=InnoDB")
        execute("CREATE TABLE dt_b (id INT PRIMARY KEY, a_id INT) ENGINE=InnoDB")
        execute("ALTER TABLE dt_a ADD FOREIGN KEY (b_id) REFERENCES dt_b(id)")
        execute("ALTER TABLE dt_b ADD FOREIGN KEY (a_id) REFERENCES dt_a(id)")
        
        dropTableWithExportedKeys("dt_a")
        tableExists("dt_a") must beFalse
        tableExists("dt_b") must beTrue
        
        jdbcModelExtractor.extractTable("dt_b").foreignKeys must beEmpty
    }
    
}

// vim: set ts=4 sw=4 et:
