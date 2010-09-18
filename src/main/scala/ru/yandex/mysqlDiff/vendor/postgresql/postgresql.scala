package ru.yandex.mysqlDiff
package vendor.postgresql

import ru.yandex.small.jdbc._

import util._

// PostgreSQL support is far from complete

object PostgresqlContext extends Context(PostgresqlDataTypes) {
    override val sqlParserCombinator = new PostgresqlParserCombinator(this)
    override val dataTypes = PostgresqlDataTypes
    override val diffSerializer = new PostgresqlDiffSerializer(this)
    override val scriptSerializer = new PostgresqlScriptSerializer(this)
    override val modelSerializer = new PostgresqlModelSerializer(this)
    override val modelParser = new PostgresqlModelParser(this)
    
    override def connectedContext(ds: LiteDataSource) = new PostgresqlConnectedContext(ds)
}

class PostgresqlConnectedContext(ds0: LiteDataSource) extends ConnectedContext(PostgresqlContext, ds0) {
    override val jdbcModelExtractor = new PostgresqlJdbcModelExtractor(this)
    override val metaDao = new PostgresqlMetaDao(jt)
}

class PostgresqlTests(includeOnline: Boolean) extends MySpecification {
    include(PostgresqlParserCombinatorTests)
    if (includeOnline) include(PostgresqlOnlineTests)
}

object PostgresqlTests extends PostgresqlTests(true)

// vim: set ts=4 sw=4 et:
