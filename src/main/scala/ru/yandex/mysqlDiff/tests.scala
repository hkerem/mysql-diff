package ru.yandex.mysqlDiff

object Tests extends org.specs.Specification {
    include(script.parser.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(model.ModelParserTests)
    include(jdbc.JdbcModelExtractorTests)
    include(script.ScriptSerializerTests)
    include(script.ScriptTests)
    include(diff.DiffMakerTests)
}

// vim: set ts=4 sw=4 et:
