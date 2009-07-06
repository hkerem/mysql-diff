package ru.yandex.mysqlDiff.vendor.postgresql

import jdbc._
import util._

class PostgresqlJdbcModelExtractor(connectedContext: ConnectedContext)
    extends JdbcModelExtractor(connectedContext)
{
    override val urlDbIsCatalog = true
    
    trait PostgresqlSchemaExtractor extends SchemaExtractor {
    }
    
    protected override def newAllTablesSchemaExtractor() =
        new AllTablesSchemaExtractor with PostgresqlSchemaExtractor {
        }
    
    protected override def newSingleTableSchemaExtractor() =
        new SingleTableSchemaExtractor with PostgresqlSchemaExtractor {
        }

}

class PostgresqlJdbcModelExtractorTests(connectedContext: ConnectedContext)
    extends JdbcModelExtractorTests(connectedContext)
{
    // nothing yet
}

// vim: set ts=4 sw=4 et:
