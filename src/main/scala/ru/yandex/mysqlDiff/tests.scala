package ru.yandex.mysqlDiff


object MysqlDiffSpecsConfiguration extends org.specs.util.Configuration {
    override def examplesWithoutExpectationsMustBePending = false
}
    
abstract class MySpecification extends org.specs.Specification {
    // evil hack
    org.specs.util.Configuration.config = MysqlDiffSpecsConfiguration
}


/** All online tests are turned off by default */
trait TestsSelector {
    def includeOnline = false
    /** Include online MySQL tests */
    def includeMysql = includeOnline
    /** Include online PostgreSQL tests */
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

class SomeTests(testsSelector: TestsSelector) extends MySpecification with CheckJavaVersion {
    include(script.SqlParserCombinatorTests)
    include(model.ModelTests)
    include(model.ModelParserTests)
    include(script.ScriptSerializerTests)
    include(script.ScriptTests)
    include(diff.DiffMakerTests)
    include(diff.DiffSerializerTests)
    include(new vendor.VendorTests(testsSelector))
    include(new jdbc.JdbcTests(testsSelector))
    include(util.CollectionTests)
    include(util.StringTests)
    include(util.getopt.GetoptTests)
    include(util.SystemInfoTests)
}

object Tests extends SomeTests(AllTestsSelector)
object OfflineTests extends SomeTests(OfflineTestsSelector)
object MysqlTests extends SomeTests(new TestsSelector { override def includeMysql = true })
object PostgresqlTests extends SomeTests(new TestsSelector { override def includePostgresql = true })

// ?
//object TestsFromTeamcity extends SomeTests(AllTestsSelector) with org.specs.runner.TeamCityReporter

object TestsFromTeamcity extends org.specs.runner.TeamCityRunner(Tests)

//class TempTests extends MySpecification
//object TestTestsWithTs extends TempTests with org.specs.runner.TeamCityReporter

// vim: set ts=4 sw=4 et:
