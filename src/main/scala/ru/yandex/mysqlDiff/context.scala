package ru.yandex.mysqlDiff

case class Context(dataTypes: model.DataTypes) {
    val diffMaker = diff.DiffMaker(this)
    val modelParser = model.ModelParser(this)
    val sqlParserCombinator = script.parser.SqlParserCombinator(this)
}

object MysqlContext extends Context(new model.MysqlDataTypes())  

object PostgresqlContext extends Context(new model.PostgresqlDataTypes())

object Environment {
    val defaultContext = MysqlContext
    
    def context(kind: String) = kind match {
        case "mysql" => MysqlContext
        case "postgresql" => PostgresqlContext
    }
}


