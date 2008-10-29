package ru.yandex.mysqlDiff

class Tests(includeOnline: Boolean) extends org.specs.Specification {
    include(script.parser.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(model.ModelParserTests)
    include(script.ScriptSerializerTests)
    include(script.ScriptTests)
    include(diff.DiffMakerTests)
    include(diff.DiffSerializerTests)
    include(new vendor.VendorTests(includeOnline))
    if (includeOnline) include(jdbc.JdbcModelExtractorTests)
}

object Tests extends Tests(true)
object OfflineTests extends Tests(false)

// vim: set ts=4 sw=4 et:
