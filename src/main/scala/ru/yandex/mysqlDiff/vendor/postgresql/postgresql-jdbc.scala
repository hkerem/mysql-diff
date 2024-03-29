package ru.yandex.mysqlDiff
package vendor.postgresql

import ru.yandex.small.jdbc._

import jdbc._
import util._
import model._

import Implicits._

class PostgresqlMetaDao(jt: JdbcTemplate) extends MetaDao(jt) {
    override def findSequenceNames(catalog: String, schema: String) =
        // XXX: where catalog, schema
        jt.query("SELECT * FROM information_schema.sequences").seq(_.getString("sequence_name"))
}

class PostgresqlJdbcModelExtractor(connectedContext: ConnectedContext)
    extends JdbcModelExtractor(connectedContext)
{
    override def urlDbIsCatalog = true
    
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
    
    "extract SEQUENCE" in {
        jt.execute("DROP SEQUENCE IF EXISTS seq_11")
        jt.execute("CREATE SEQUENCE seq_11")
        val db = jdbcModelExtractor.extract()
        db.sequence("seq_11").name must_== "seq_11"
    }
}

// vim: set ts=4 sw=4 et:
