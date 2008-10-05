package ru.yandex.mysqlDiff.vendor.mysql

import scalax.control.ManagedResource
import java.sql._

import jdbc._
import diff._
import model._
import script._

object MysqlTestDataSourceParameters {
    Class.forName("com.mysql.jdbc.Driver")
    
    val testDsUrl = "jdbc:mysql://fastshot:3306/mysql_diff_test"
    val testDsUser = "test"
    val testDsPassword = "test"
    
    def openConnection() = DriverManager.getConnection(testDsUrl, testDsUser, testDsPassword)
    
    val conn = ManagedResource(openConnection)
    
    val jdbcTemplate = new util.JdbcTemplate(() => openConnection())
}

object MysqlOnlineTests extends org.specs.Specification {
    import MysqlTestDataSourceParameters._
    
    "CAP-101" in {
        jdbcTemplate.execute("DROP TABLE IF EXISTS a")
        
        jdbcTemplate.execute("CREATE TABLE a (kk INT)")
        val nk = "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, kk INT)"
        val oldModel = JdbcModelExtractor.extractTable("a", conn)
        val newModel = ModelParser.parseCreateTableScript(nk)
        val diff = DiffMaker.compareTables(oldModel, newModel).get
        val script = new Script(TableScriptBuilder.alterScript(diff, newModel)).statements
        //script.foreach((s: ScriptStatement) => println(s.serialize))
        script.foreach((s: ScriptStatement) => jdbcTemplate.execute(s.serialize))
        val gotModel = JdbcModelExtractor.extractTable("a", conn)
        // XXX: check model
        ()
    }
}

// vim: set ts=4 sw=4 et:
