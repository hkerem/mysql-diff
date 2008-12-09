package ru.yandex.mysqlDiff.vendor.postgresql

object PostgresqlContext extends Context(PostgresqlDataTypes)

class PostgresqlTests(includeOnline: Boolean) extends org.specs.Specification {
}

object PostgresqlTests extends PostgresqlTests(true)

// vim: set ts=4 sw=4 et:
