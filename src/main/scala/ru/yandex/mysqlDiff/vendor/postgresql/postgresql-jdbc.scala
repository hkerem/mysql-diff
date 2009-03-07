package ru.yandex.mysqlDiff.vendor.postgresql

import jdbc._
import util._

class PostgresqlJdbcModelExtractor(context: Context) extends JdbcModelExtractor(context) {
    trait PostgresqlSchemaExtractor extends SchemaExtractor {
        override val urlDbIsCatalog = true
    }
    
    protected override def newAllTablesSchemaExtractor(jt: JdbcTemplate) =
        new AllTablesSchemaExtractor(jt) with PostgresqlSchemaExtractor {
        }
    
    protected override def newSingleTableSchemaExtractor(jt: JdbcTemplate) =
        new SingleTableSchemaExtractor(jt) with PostgresqlSchemaExtractor {
        }

}

// vim: set ts=4 sw=4 et:
