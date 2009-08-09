package ru.yandex.mysqlDiff.vendor.postgresql

import model._
import script._

class PostgresqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override def dataTypeName =
        ("DOUBLE" ~ "PRECISION" ^^ { case x ~ y => x + " " + y }
        |"CHARACTER" ~ "VARYING" ^^ { case x ~ y => x + " " + y }
        | super.dataTypeName
        )

    
    override def sqlExpr: Parser[SqlExpr] = super.sqlExpr <~ opt("::" ~ name)
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
    
    "NUMERIC(20, 6)" in {
        parse(dataType)("NUMERIC(20, 6)") must beLike {
            case NumericDataType(Some(20), Some(6)) => true
            case _ => false
        }
        
        parse(dataType)("NUMERIC(17)") must beLike {
            case NumericDataType(Some(17), None) => true
            case _ => false
        }
        
        parse(dataType)("NUMERIC") must beLike {
            case NumericDataType(None, None) => true
            case _ => false
        }
    }
    
    "numeric constants" in {
        // from http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html
        val cs = Seq("42", "3.5", "4.", ".001", "5e2", "5e-2", "1.23e3", "1.925e-3")
        for (c <- cs)
            parse(numberValue)(c) // just parse for a while
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
    
    "parse nextval regclass" in {
        val e = parse(sqlExpr)("nextval('id_seq'::regclass)").asInstanceOf[FunctionCallExpr]
        e.name must_== "nextval"
        e.params must beLike { case Seq(StringValue("id_seq")) => true }
    }
    
}

// vim: set ts=4 sw=4 et:
