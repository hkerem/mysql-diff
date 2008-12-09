package ru.yandex.mysqlDiff.vendor

class VendorTests(testsSelector: TestsSelector) extends org.specs.Specification {
    include(new mysql.MysqlTests(testsSelector.includeMysql))
    include(new postgresql.PostgresqlTests(testsSelector.includePostgresql))
}

object VendorTests extends VendorTests(AllTestsSelector)

// vim: set ts=4 sw=4 et:
