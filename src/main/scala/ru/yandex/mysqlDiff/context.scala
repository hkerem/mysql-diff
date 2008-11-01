package ru.yandex.mysqlDiff

case class Context(dataTypes: model.DataTypes) {
    val diffMaker = diff.DiffMaker(this)
    val modelParser = model.ModelParser(this)
    val sqlParserCombinator = script.parser.SqlParserCombinator(this)
}

object PostgresqlContext extends Context(new model.PostgresqlDataTypes())

object Environment {
    val defaultContext = vendor.mysql.MysqlContext
    
    def context(kind: String) = kind match {
        case "mysql" => vendor.mysql.MysqlContext
        case "postgresql" => PostgresqlContext
    }
}

// vim: set ts=4 sw=4 et:
