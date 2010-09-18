package ru.yandex.mysqlDiff
package vendor

class VendorTests(testsSelector: TestsSelector) extends MySpecification {
    include(new mysql.MysqlTests(testsSelector.includeMysql))
    include(new postgresql.PostgresqlTests(testsSelector.includePostgresql))
}

object VendorTests extends VendorTests(AllTestsSelector)

// vim: set ts=4 sw=4 et:
