package ru.yandex.mysqlDiff

case class Context(dataTypes: model.DataTypes) {
    val diffMaker = new diff.DiffMaker(this)
    val modelParser = new model.ModelParser(this)
    val sqlParserCombinator = new script.parser.SqlParserCombinator(this)
    val parser = new script.parser.Parser(this)
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
