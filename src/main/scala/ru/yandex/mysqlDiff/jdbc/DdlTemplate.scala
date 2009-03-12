package ru.yandex.mysqlDiff.jdbc

import script._

class DdlTemplate(connectedContext: ConnectedContext) {
    import connectedContext._
    import context._
    
    def execute(s: ScriptElement) =
        jt.execute(scriptSerializer.serialize(s))
    
    def dropTable(name: String) =
        execute(new DropTableStatement(name, false))
    
    def dropTableIfExists(name: String) =
        execute(new DropTableStatement(name, true))
    
    // not tested
    def tableExists(name: String) = {
        val tableNames = metaDao.findTableNames(
            jdbcModelExtractor.currentCatalog, jdbcModelExtractor.currentSchema)
        tableNames.map(_.toUpperCase) contains name.toUpperCase
    }
    
    /** DROP TABLE IF EXISTS, then execute script */
    def recreateTable(script: String) = {
        val cts = sqlParserCombinator.parseCreateTableRegular(script)
        dropTableIfExists(cts.name)
        jt.execute(script)
    }
    
}

// vim: set ts=4 sw=4 et:
