package ru.yandex.mysqlDiff.jdbc

import script._

import Implicits._

class DdlTemplate(connectedContext: ConnectedContext) {
    import connectedContext._
    import context._
    
    private def executeRaw(s: String) =
        try {
            jt.execute(s)
        } catch {
            case e: Exception =>
                throw new MysqlDiffException("failed to execute script: "+ e +": "+ s, e)
        }
    
    def execute(s: ScriptElement) =
        executeRaw(scriptSerializer.serialize(s))
    
    def executeScript(s: String) = {
        val trimmed = s.replaceFirst(";\\s*$", "")
        if (trimmed contains ";") {
            for (e <- parser.parse(trimmed).statements)
                execute(e)
        } else {
            executeRaw(trimmed)
        }
    }
    
    def dropTable(name: String) =
        execute(new DropTableStatement(name, false))
    
    def dropTableIfExists(name: String) =
        execute(new DropTableStatement(name, true))
    
    /**
     * DROP TABLE itself and DROP FOREIGN KEYS exported by this table.
     */
    def dropTableWithExportedKeys(name: String) = {
        val exportedKeys =
            metaDao.findExportedKeys(jdbcModelExtractor.currentCatalog, jdbcModelExtractor.currentSchema, name)
                .map(ref => (ref.fkTableName, ref.fkName)).unique
        for ((fkTable, fkKey) <- exportedKeys) {
            execute(new AlterTableStatement(fkTable, Seq(TableDdlStatement.DropForeignKey(fkKey.get))))
        }
        dropTable(name)
    }
    
    def dropTableWithExportedKeysIfExists(name: String) =
        if (tableExists(name)) dropTableWithExportedKeys(name)
    
    // not tested
    def tableExists(name: String) = {
        val tableNames = metaDao.findTableNames(
            jdbcModelExtractor.currentCatalog, jdbcModelExtractor.currentSchema)
        tableNames.map(_.toUpperCase) contains name.toUpperCase
    }
    
    /** DROP TABLE IF EXISTS, then execute script */
    def recreateTable(script: String) = {
        val cts = sqlParserCombinator.parseCreateTable(script)
        dropTableIfExists(cts.name)
        jt.execute(script)
    }
    
}

class DdlTemplateTests(connectedContext: ConnectedContext) extends org.specs.Specification {
    import connectedContext._
    import connectedContext.context._
    
    import ddlTemplate._
    import jt._
    
    "tableExists" in {
        recreateTable("CREATE TABLE table_exists_test (id INT)")
        tableExists("table_exists_test") must beTrue
        dropTableIfExists("table_exists_test")
        tableExists("table_exists_test") must beFalse
        dropTableIfExists("table_exists_test")
    }
}

// vim: set ts=4 sw=4 et:
