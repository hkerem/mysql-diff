package ru.yandex.mysqlDiff

import ru.yandex.small.jdbc.LiteDataSource
import ru.yandex.small.jdbc.JdbcTemplate

import util._

import Implicits._

/**
 * Context holds references to DB-specific implementations of algorithms.
 */
class Context(val dataTypes: model.DataTypes) {
    val diffMaker = new diff.DiffMaker(this)
    val diffSerializer = new diff.DiffSerializer(this)
    val modelParser = new model.ModelParser(this)
    val modelSerializer = new model.ModelSerializer(this)
    val sqlParserCombinator = new script.SqlParserCombinator(this)
    val parser = new script.Parser(this)
    val scriptSerializer = new script.ScriptSerializer(this)
    
    def connectedContext(ds: LiteDataSource) = new ConnectedContext(this, ds)
    val utils = new Utils(this)
}

class ConnectedContext(val context: Context, ds0: LiteDataSource) {
    val dsVar = new scala.util.DynamicVariable(ds0)
    val ds = new LiteDataSource {
        override def openConnection() = dsVar.value.openConnection()
        override def closeConnection(c: java.sql.Connection) = dsVar.value.closeConnection(c)
        override def close() = dsVar.value.close()
    }
    val jt = new JdbcTemplate(ds)
    val jdbcModelExtractor = new jdbc.JdbcModelExtractor(this)
    val metaDao = new jdbc.MetaDao(jt)
    val ddlTemplate = new jdbc.DdlTemplate(this)
}

// XXX: create basic context

// XXX: rename to Context
object Environment {
    val defaultContext = vendor.mysql.MysqlContext
    
    def context(kind: String) = kind.toLowerCase match {
        case "postgresql" => vendor.postgresql.PostgresqlContext
        case "mysql" | "default" => defaultContext
    }
    
    def contextByUrl(url: String) = {
        require(url startsWith "jdbc:")
        context(url.replaceFirst("^jdbc:", "").replaceFirst(":.*", ""))
    }
}

// vim: set ts=4 sw=4 et:
