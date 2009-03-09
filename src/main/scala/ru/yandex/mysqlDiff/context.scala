package ru.yandex.mysqlDiff

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
    val jdbcModelExtractor = new jdbc.JdbcModelExtractor(this)
}

// XXX: create basic context

object Environment {
    val defaultContext = vendor.mysql.MysqlContext
    
    def context(kind: String) = kind match {
        case "mysql" => vendor.mysql.MysqlContext
        case "postgresql" => vendor.postgresql.PostgresqlContext
    }
}

// vim: set ts=4 sw=4 et:
