package ru.yandex.mysqlDiff.vendor.postgresql

import jdbc._
import util._
import model._

import Implicits._

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
    import connectedContext._
    import connectedContext.context._
    
    "DEFAULT NULL for VARCHAR(100)" in {
        ddlTemplate.recreateTable("CREATE TABLE powerbooks (col VARCHAR(100))")
        val t = extractTable("powerbooks")
        t.column("col").defaultValue must_== Some(NullValue)
    }
}

// vim: set ts=4 sw=4 et:
