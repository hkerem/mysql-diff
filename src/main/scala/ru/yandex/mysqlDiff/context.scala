package ru.yandex.mysqlDiff

class Context(val dataTypes: model.DataTypes) {
    val diffMaker = new diff.DiffMaker(this)
    val modelParser = new model.ModelParser(this)
    val sqlParserCombinator = new script.parser.SqlParserCombinator(this)
    val parser = new script.parser.Parser(this)
}

object Environment {
    val defaultContext = vendor.mysql.MysqlContext
    
    def context(kind: String) = kind match {
        case "mysql" => vendor.mysql.MysqlContext
        case "postgresql" => vendor.postgresql.PostgresqlContext
    }
}

// vim: set ts=4 sw=4 et:
