package ru.yandex.mysqlDiff.vendor.postgresql

import util._

// PostgreSQL support is far from complete

object PostgresqlContext extends Context(PostgresqlDataTypes) {
    override val sqlParserCombinator = new PostgresqlParserCombinator(this)
    override val dataTypes = PostgresqlDataTypes
    override val diffSerializer = new PostgresqlDiffSerializer(this)
    
    override def connectedContext(ds: LiteDataSource) = new PostgresqlConnectedContext(ds)
}

class PostgresqlConnectedContext(ds: LiteDataSource) extends ConnectedContext(PostgresqlContext, ds) {
    override val jdbcModelExtractor = new PostgresqlJdbcModelExtractor(this)
}

class PostgresqlTests(includeOnline: Boolean) extends org.specs.Specification {
    include(PostgresqlParserCombinatorTests)
    if (includeOnline) include(PostgresqlOnlineTests)
}

object PostgresqlTests extends PostgresqlTests(true)

// vim: set ts=4 sw=4 et:
