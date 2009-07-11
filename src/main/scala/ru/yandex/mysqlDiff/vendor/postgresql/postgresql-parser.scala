package ru.yandex.mysqlDiff.vendor.postgresql

import model._
import script._

class PostgresqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override def dataTypeName = (("DOUBLE" ~ "PRECISION") ^^ { case x ~ y => x + " " + y }) | super.dataTypeName
}

object PostgresqlParserCombinatorTests extends SqlParserCombinatorTests(PostgresqlContext) {
    val context = PostgresqlContext
    
    import context._
    import sqlParserCombinator._
    
    "DOUBLE PRECISION" in {
        parse(dataType)("DOUBLE PRECISION") must beLike {
            case DefaultDataType("DOUBLE PRECISION", None) => true
            case _ => false
        }
    }
    
    // this test does not work for MySQL
    "parse FK" in {
        val t = parseCreateTable(
                "CREATE TABLE a (id INT, " +
                "FOREIGN KEY (x, y) REFERENCES b (x1, y1), " +
                "CONSTRAINT fk1 FOREIGN KEY (z) REFERENCES c (z1))")
        t.foreignKeys must haveSize(2)
        t.foreignKeys(0).fk must beLike {
            case ForeignKeyModel(None, Seq(IndexColumn("x", _, _), IndexColumn("y", _, _)), "b", Seq("x1", "y1"), _, _) => true
            case _ => false
        }
        t.foreignKeys(1).fk must beLike {
            case ForeignKeyModel(Some("fk1"), Seq(IndexColumn("z", _, _)), "c", Seq("z1"), _, _) => true
            case _ => false
        }
    }
    
}

// vim: set ts=4 sw=4 et:
