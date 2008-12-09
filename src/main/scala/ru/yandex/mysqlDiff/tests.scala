package ru.yandex.mysqlDiff

/** All online tests are turned off by default */
trait TestsSelector {
    def includeOnline = false
    def includeMysql = includeOnline
    def includePostgresql = includeOnline
}

/** All tests */
object AllTestsSelector extends TestsSelector {
    override def includeOnline = true
}

/** Only offline tests */
object OfflineTestsSelector extends TestsSelector {
    override def includeOnline = false
}

class SomeTests(testsSelector: TestsSelector) extends org.specs.Specification {
    include(script.parser.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(model.ModelParserTests)
    include(script.ScriptSerializerTests)
    include(script.ScriptTests)
    include(diff.DiffMakerTests)
    include(diff.DiffSerializerTests)
    include(new vendor.VendorTests(testsSelector))
    // XXX: JdbcModelExtractorTests must be moved to mysql subpackage
    if (testsSelector.includeMysql) include(jdbc.JdbcModelExtractorTests)
}

object Tests extends SomeTests(AllTestsSelector)
object OfflineTests extends SomeTests(OfflineTestsSelector)
object MysqlTests extends SomeTests(new TestsSelector { override def includeMysql = true })
object PostgresqlTests extends SomeTests(new TestsSelector { override def includePostgresql = true })

// vim: set ts=4 sw=4 et:
