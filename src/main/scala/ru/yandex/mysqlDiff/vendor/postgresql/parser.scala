package ru.yandex.mysqlDiff.vendor.postgresql

import model._
import script.parser._

class PostgresqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override def dataTypeName = (("DOUBLE" ~ "PRECISION") ^^ { case x ~ y => x + " " + y }) | super.dataTypeName
}

object PostgresqlParserCombinatorTests extends SqlParserCombinatorTests(PostgresqlContext) {
    val context = PostgresqlContext
    
    import context._
    import sqlParserCombinator._
    
    "DOUBLE PRECISION" in {
        parse(dataType)("DOUBLE PRECISION") must_== PostgresqlDataType("DOUBLE PRECISION", None, Nil)
    }
}

// vim: set ts=4 sw=4 et:
