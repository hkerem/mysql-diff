package ru.yandex.mysqlDiff

object Tests extends org.specs.Specification {
    include(script.parser.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(jdbc.JdbcModelExtractorTests)
}

// vim: set ts=4 sw=4 et:
