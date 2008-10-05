package ru.yandex.mysqlDiff

object OfflineTests extends org.specs.Specification {
    include(script.parser.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(model.ModelParserTests)
    include(script.ScriptSerializerTests)
    include(script.ScriptTests)
    include(diff.DiffMakerTests)
    include(diff.DiffSerializerTests)
    include(vendor.VendorTests)
}

object Tests extends org.specs.Specification {
    include(OfflineTests)
    include(jdbc.JdbcModelExtractorTests)
}

// vim: set ts=4 sw=4 et:
