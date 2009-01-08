package ru.yandex.mysqlDiff.vendor.postgresql

object PostgresqlContext extends Context(PostgresqlDataTypes) {
    override val sqlParserCombinator = new PostgresqlParserCombinator(this)
}

class PostgresqlTests(includeOnline: Boolean) extends org.specs.Specification {
    include(PostgresqlParserCombinatorTests)
}

object PostgresqlTests extends PostgresqlTests(true)

// vim: set ts=4 sw=4 et:
