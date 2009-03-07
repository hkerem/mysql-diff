package ru.yandex.mysqlDiff.vendor.postgresql

// PostgreSQL support is far from complete

object PostgresqlContext extends Context(PostgresqlDataTypes) {
    override val sqlParserCombinator = new PostgresqlParserCombinator(this)
    override val jdbcModelExtractor = new PostgresqlJdbcModelExtractor(this)
    override val dataTypes = PostgresqlDataTypes
}

class PostgresqlTests(includeOnline: Boolean) extends org.specs.Specification {
    include(PostgresqlParserCombinatorTests)
    if (includeOnline) include(PostgresqlOnlineTests)
}

object PostgresqlTests extends PostgresqlTests(true)

// vim: set ts=4 sw=4 et:
