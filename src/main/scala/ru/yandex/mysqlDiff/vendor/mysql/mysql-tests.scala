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
    
    override val scriptPreamble = "SET storage_engine = InnoDB"
    
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
        execute("CREATE TABLE datacenters (id INT PRIMARY KEY)")
        val t2 = checkTwoTables(
            "CREATE TABLE servers (id INT, dc_id INT)",
            "CREATE TABLE servers (id INT, dc_id INT, " +
                    "CONSTRAINT dc_fk FOREIGN KEY dc_idx (dc_id) REFERENCES datacenters(id))")
        t2.foreignKeys must haveSize(1)
    }
    
    "FOREIGN KEY with overlapping INDEX" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("yyyy")
        ddlTemplate.dropTableWithExportedKeysIfExists("zzzz")
        recreateTable("CREATE TABLE zzzz (id INT PRIMARY KEY)")
        checkTwoTables(
            "CREATE TABLE yyyy (id INT, zzzz_id INT, CONSTRAINT zzzz_c FOREIGN KEY zzzz_i (zzzz_id) REFERENCES zzzz(id), INDEX(zzzz_id, id))",
            "CREATE TABLE yyyy (id INT, zzzz_id INT, CONSTRAINT zzzz_c FOREIGN KEY zzzz_i (zzzz_id) REFERENCES zzzz(id))"
            )
    }
    
    "FOREIGN KEY with overlapping UNIQUE" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("uuuu")
        ddlTemplate.dropTableWithExportedKeysIfExists("qqqq")
        recreateTable("CREATE TABLE qqqq (id INT PRIMARY KEY)")
        checkTwoTables(
            "CREATE TABLE uuuu (id INT, qqqq_id INT, CONSTRAINT qqqq_c FOREIGN KEY qqqq_i (qqqq_id) REFERENCES qqqq(id), UNIQUE(qqqq_id, id))",
            "CREATE TABLE uuuu (id INT, qqqq_id INT, CONSTRAINT qqqq_c FOREIGN KEY qqqq_i (qqqq_id) REFERENCES qqqq(id))"
            )
    }
    
    "FOREIGN KEY with overlapping PRIMARY KEY" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("mmmm")
        ddlTemplate.dropTableWithExportedKeysIfExists("nnnn")
        recreateTable("CREATE TABLE mmmm (id INT PRIMARY KEY)")
        checkTwoTables(
            "CREATE TABLE nnnn (id INT, mmmm_id INT, CONSTRAINT mmmm_c FOREIGN KEY mmmm_i (mmmm_id) REFERENCES mmmm(id), PRIMARY KEY(mmmm_id, id))",
            "CREATE TABLE nnnn (id INT, mmmm_id INT, CONSTRAINT mmmm_c FOREIGN KEY mmmm_i (mmmm_id) REFERENCES mmmm(id))"
            )
    }
    
    "FOREIGN KEY ... ON UPDATE ... ON DELETE" in {
        ddlTemplate.dropTableWithExportedKeysIfExists("o_u_o_d")
        ddlTemplate.dropTableWithExportedKeysIfExists("o_u_o_d_pri")
        execute("CREATE TABLE o_u_o_d_pri (id INT PRIMARY KEY)")
        checkTwoTables(
            "CREATE TABLE o_u_o_d (a_id INT, id INT, " +
                "FOREIGN KEY (a_id) REFERENCES o_u_o_d_pri(id) ON UPDATE SET NULL)",
            "CREATE TABLE o_u_o_d (a_id INT, id INT, " +
                "FOREIGN KEY (a_id) REFERENCES o_u_o_d_pri(id) ON UPDATE CASCADE ON DELETE SET NULL)"
            )
    }
    
    "diff, apply collate" in {
        jt.execute("DROP TABLE IF EXISTS collate_test")
        execute("CREATE TABLE collate_test (id VARCHAR(10)) COLLATE=cp1251_bin")
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
        execute("CREATE TABLE moderated_tags (tag VARCHAR(255) CHARACTER SET utf8 NOT NULL, type INT(11) NOT NULL) DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
        val s2 = "CREATE TABLE moderated_tags (tag VARCHAR(255) NOT NULL, type INT(11) NOT NULL) DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
        
        val dbModel = jdbcModelExtractor.extractTable("moderated_tags")
        val localModel = modelParser.parseCreateTableScript(s2)
        
        // column collation is changed from utf8_bin to utf8_general_ci
        // (character set implies collation)
        diffMaker.compareTables(dbModel, localModel) must beLike { case Some(_) => true }
    }
    
    "bug with data type options equivalence" in {
        checkTable(
            "CREATE TABLE alive_apps (hostname VARCHAR(100) NOT NULL) DEFAULT CHARSET=utf8")
    }
    
    "bug with COLLATE before CHARACTER SET" in {
        checkTable(
            "CREATE TABLE IF NOT EXISTS tag_cloud_global (tag varchar(64) collate utf8_bin) DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
    }
    
    "complex bug with charset and collation" in {
        checkTable(
            """
            CREATE TABLE charset_collation (zz VARCHAR(3) CHARACTER SET latin1) ENGINE=InnoDB DEFAULT CHARSET=utf8
            """)
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
    
    "BIT" in {
        checkTable(
            """CREATE TABLE stud (aud BIT NOT NULL DEFAULT '\0')""")
    }
    
    "TINYINT(1) DEFAULT '1'" in {
        checkTable(
            """CREATE TABLE tinyint_1 (a TINYINT(1) NOT NULL DEFAULT '1')""")
    }
    
    // http://bitbucket.org/stepancheg/mysql-diff/issue/24/default-value-for-tinyint
    "TINYINT(4)" in {
        checkTwoTables(
            "CREATE TABLE tinyint_4 (xx TINYINT(4) NOT NULL DEFAULT '0')",
            "CREATE TABLE tinyint_4 (xx TINYINT(4) NOT NULL DEFAULT '3')"
            )
    }
    
    "DECIMAL DEFAULT VALUE" in {
        checkTable("CREATE TABLE decimal_dv (id INT, deci DECIMAL(10, 6) DEFAULT '0.000000')")
    }
    
    "MEDIUMINT UNSIGNED" in {
        checkTable(
            "CREATE TABLE service_with_mediumint_unsigned (bg_color MEDIUMINT UNSIGNED)")
    }
    
    "DEFAULT DATE 0000-00-00" in {
        checkTable(
            "CREATE TABLE date_0000000 (d DATE DEFAULT '0000-00-00')")
    }
    
    "DEFAULT DATE 0" in {
        checkTable(
            "CREATE TABLE date_0 (d DATE DEFAULT 0)")
    }
    
    "DEFAULT TIME 0" in {
        checkTable(
            "CREATE TABLE time_0 (d TIME DEFAULT 0)")
    }
    
    "DEFAULT DATETIME 0" in {
        checkTable(
            "CREATE TABLE datetime_0 (d DATETIME DEFAULT 0)")
    }
    
    "DEFAULT TIMESTAMP 0" in {
        // XXX: test is broken
        checkTable(
            "CREATE TABLE timestamp_0 (d TIMESTAMP DEFAULT 0)")
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
        val version = jt.query("SELECT @@version").single(rs => MysqlServerVersion.parse(rs.getString(1)))
        if (version < MysqlServerVersion(5, 1, 0)) {
            checkTwoDatabases(
                """
                CREATE TABLE nu_a (id INT PRIMARY KEY);
                CREATE TABLE nu_b (a_id INT);
                CREATE TABLE nu_c (a_id INT, CONSTRAINT c866 FOREIGN KEY zc (a_id) REFERENCES nu_a(id))
                """,
                """
                CREATE TABLE nu_a (id INT PRIMARY KEY);
                CREATE TABLE nu_b (a_id INT, CONSTRAINT c866 FOREIGN KEY zb (a_id) REFERENCES nu_a(id));
                CREATE TABLE nu_c (a_id INT)
                """)
        }
    }
    
    "MySQL 5.1 weird CONSTRAINT name FOREIGN KEY name behaviour" in {
        // MySQL 5.1 seems to use constraint name for foreign key name
        checkDatabase(
            """
            CREATE TABLE nu_a (id INT PRIMARY KEY);
            CREATE TABLE nu_b (a_id INT, CONSTRAINT c866 FOREIGN KEY zb (a_id) REFERENCES nu_a(id));
            CREATE TABLE nu_c (a_id INT)
            """
        )
    }
    
    "bug with ADD FOREIGN KEY referenced to newly created table" in {
        checkTwoDatabases(
            """
            CREATE TABLE cities (id INT PRIMARY KEY)
            """,
            """
            CREATE TABLE countries (id INT PRIMARY KEY);
            CREATE TABLE cities (id INT PRIMARY KEY, cid INT, CONSTRAINT ccid FOREIGN KEY icid (cid) REFERENCES countries(id))
            """)
    }
    
    "with FOREIGN KEY unchanged" in {
        dropTable("coins")
        dropTable("collectors")
        
        execute("CREATE TABLE collectors (id INT PRIMARY KEY)")
        checkTable(
            "CREATE TABLE coins (coll_id INT," +
                " CONSTRAINT coin_fk FOREIGN KEY (coll_id) REFERENCES collectors(id))")
    }
    
    "UNIQUE unchanged" in {
        checkTable(
            "CREATE TABLE bears (id INT, name VARCHAR(10), CONSTRAINT name_uniq UNIQUE(name))")
    }
    
    "CREATE INDEX" in {
        checkTwoTables(
            """
            CREATE TABLE movies (id INT, name VARCHAR(100))
            """,
            """
            CREATE TABLE movies (id INT, name VARCHAR(100));
            CREATE INDEX movies_name_idx ON movies (name)
            """
        )
    }
    
    // http://bitbucket.org/stepancheg/mysql-diff/issue/21/support-indexes-with-leading-part
    "INDEX with leading part" in {
        checkTwoTables(
            """
            CREATE TABLE index_lp (name VARCHAR(100), INDEX(name))
            """,
            """
            CREATE TABLE index_lp (name VARCHAR(100), INDEX(name(20)))
            """
        )
    }
    
    "PRIMARY KEY indexing leading part" in {
        checkTwoTables(
            """
            CREATE TABLE pk_lp (name VARCHAR(100), PRIMARY KEY (name(9)))
            """,
            """
            CREATE TABLE pk_lp (name VARCHAR(100), PRIMARY KEY (name))
            """
        )
    }
    
    "TABLE AUTO_INCREMENT" in {
        checkTable(
            """
            CREATE TABLE table_auto_inc (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT) ENGINE=MyISAM AUTO_INCREMENT=11
            """
        )
    }
    
    "TABLE AUTO_INCREMENT with increment" in {
        dropTable("aiwi")
        dropTable("aiwi2")
        val script = "CREATE TABLE aiwi (ID INT PRIMARY KEY AUTO_INCREMENT, v INT) ENGINE=MyISAM AUTO_INCREMENT=122"
        val script2 = script.replaceFirst("aiwi", "aiwi2")
        
        execute(script)
        execute(script2)
        
        execute("INSERT INTO aiwi (v) VALUES (1)")
        val dbModel = jdbcModelExtractor.extractTable("aiwi")
        val localModel = modelParser.parseCreateTableScript(script)
        diffMaker.compareTables(dbModel, localModel) must beLike { case None => true }
        diffMaker.compareTables(localModel, dbModel) must beLike { case None => true }
        
        val dbModel2 = jdbcModelExtractor.extractTable("aiwi2").withName("aiwi")
        diffMaker.compareTables(dbModel, dbModel2) must beLike { case None => true }
        diffMaker.compareTables(dbModel2, dbModel) must beLike { case None => true }
    }
    
    "TABLE change COMMENT" in {
        checkTwoTables(
            "CREATE TABLE table_comment_1 (id INT) COMMENT 'paper'",
            "CREATE TABLE table_comment_1 (id INT) COMMENT 'scissors'"
        )
    }
    
    "TABLE add COMMENT" in {
        checkTwoTables(
            "CREATE TABLE table_comment_2 (id INT)",
            "CREATE TABLE table_comment_2 (id INT) COMMENT 'scissors'"
        )
    }
    
    "COLUMN COMMENT" in {
        checkTwoTables(
            "CREATE TABLE bottles (height INT COMMENT 'in cm')",
            "CREATE TABLE bottles (height INT)"
        )
    }
    
    "ENGINE copied in CREATE TABLE LIKE" in {
        // test repeated twice because I don't know what default engine is
        checkDatabase(
            """
            CREATE TABLE engine_copied_template (id INT) ENGINE=MyISAM;
            CREATE TABLE engine_copied LIKE engine_copied_template
            """
        )
        checkDatabase(
            """
            CREATE TABLE engine_copied_template (id INT) ENGINE=InnoDB;
            CREATE TABLE engine_copied LIKE engine_copied_template
            """
        )
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
