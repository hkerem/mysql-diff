package ru.yandex.mysqlDiff.script.parser

import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import scalax.io._

class SqlLexical extends StdLexical {
    // ?
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    override def letter = elem("letter", x => x.isLetter || x == '_' || x == '%') // ?
    
    //override def token: Parser[Token] =
    //    letter ~ rep(letter | digit)
    
    override def whitespace: Parser[Any] = rep(
        whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ comment
      | ('-' ~ '-' ~ rep(chrExcept(EofCh, '\n')))
    )
    
    protected override def processIdent(name: String) =
        if (reserved contains name.toUpperCase) Keyword(name.toUpperCase) else Identifier(name)
}

object Sql extends StandardTokenParsers {
    override val lexical = new SqlLexical
    
    lexical.delimiters += ("(", ")", "=", ",", ";", "=")
    
    lexical.reserved += ("CREATE", "TABLE", "VIEW", "IF", "NOT", "NULL", "EXISTS",
            "AS", "SELECT", "UNIQUE", "KEY", "INDEX", "PRIMARY", "DEFAULT", "NOW", "AUTO_INCREMENT")
   
    abstract class SqlValue
    
    case object NullValue extends SqlValue
    
    case class NumberValue(value: Int) extends SqlValue
    
    case class StringValue(value: String) extends SqlValue
    
    case object Now extends SqlValue
    
    // should be SqlValue
    def nullValue: Parser[SqlValue] = "NULL" ^^ { x => NullValue }
    
    def numberValue: Parser[NumberValue] = numericLit ^^ { x => NumberValue(x.toInt) } // silly
    
    def stringValue: Parser[StringValue] = stringLit ^^
            { x => StringValue(x.replaceFirst("^[\"']", "").replaceFirst("[\"']$", "")) }
    
    def nowValue: Parser[SqlValue] = "NOW" ~ "(" ~ ")" ^^ { x => Now }
    
    def sqlValue: Parser[SqlValue] = nullValue | numberValue | stringValue | nowValue
    
    case class DataType(name: String, length: Option[Int])
    
    def dataType: Parser[Any] = name ~ opt("(" ~> numericLit <~ ")") ^^
            { case name ~ length => DataType(name, length.map(_.toInt)) }
   
    abstract class TableEntry
    
    abstract class ColumnProperty
    
    case class Nullable(nullable: Boolean) extends ColumnProperty
    
    def nullability: Parser[Nullable] = opt("NOT") <~ "NULL" ^^ { x => Nullable(x.isEmpty) }
    
    case class DefaultValue(value: SqlValue) extends ColumnProperty
    
    def defaultValue: Parser[DefaultValue] = "DEFAULT" ~> sqlValue ^^ { value => DefaultValue(value) }
    
    case object AutoIncrement extends ColumnProperty
    
    // Parser[AutoIncrement.type] does not work
    def autoIncrementability: Parser[ColumnProperty] = "AUTO_INCREMENT" ^^ { x => AutoIncrement }
    
    case class Column(name: String, dataType: Any, attrs: Seq[ColumnProperty]) extends TableEntry
    
    def columnAttr = nullability | defaultValue | autoIncrementability
    
    def column: Parser[Column] = name ~ dataType ~ rep(columnAttr) ^^
            { case name ~ dataType ~ attrs => Column(name, dataType, attrs) }
    
    def name: Parser[String] = ident
    
    def nameList: Parser[Seq[String]] = "(" ~> repsep(name, ",") <~ ")"
    
    def trash: Parser[Any] = name | "=" | "DEFAULT"
    
    //def select: Parser[Any] = "SELECT" ~> rep(not(";" | lexical.EOF))
    def select: Parser[Any] = "SELECT" ~> rep(name)
    
    case class Index(name: Option[String], unique: Boolean, columnNames: Seq[String]) extends TableEntry
    
    // XXX: why need braces?
    def index: Parser[Index] = (opt("UNIQUE") <~ ("INDEX" | "KEY")) ~ opt(name) ~ nameList ^^
            { case unique ~ name ~ columnNames => Index(name, unique.isDefined, columnNames) }
    
    case class PrimaryKey(columnNames: Seq[String]) extends TableEntry
    
    def pk: Parser[PrimaryKey] = "PRIMARY" ~> "KEY" ~> nameList ^^ { nameList => PrimaryKey(nameList) }
    
    def tableEntry: Parser[TableEntry] = column | index | pk
    
    def ifNotExists: Parser[Any] = "IF" ~ "NOT" ~ "EXISTS"
    
    //def tableOptions: Parser[Any] = 
    
    case class CreateTable(name: String, ifNotExists: Boolean, entires: Seq[TableEntry])
    
    // XXX: parse table options
    def createTable: Parser[CreateTable] = "CREATE" ~> "TABLE" ~> opt(ifNotExists) ~ name ~
            ("(" ~> repsep(tableEntry, ",") <~ ")") ~ rep(trash) ^^
                    { case ifne ~ name ~ entries ~ _ => CreateTable(name, ifne.isDefined, entries) }
    
    def createView: Parser[Any] = "CREATE" ~ "VIEW" ~ opt(ifNotExists) ~ name ~ opt(nameList) ~ "AS" ~ select
    
    def topLevel: Parser[Any] = createView | createTable
    
    def script: Parser[Any] = repsep(topLevel, ";") <~ opt(";") ~ lexical.EOF
    
    def main(args: Array[String]) {
        val text =
            if (args.length == 1) {
                args(0)
            } else {
                ReaderResource.apply(args(1)).slurp()
            }
        val tokens = new lexical.Scanner(text)
        println(phrase(script)(tokens))
        //println(phrase(script)(tokens).getClass)
        //println(phrase(script)(tokens).get)
        //println(phrase(script)(tokens).get.asInstanceOf[AnyRef].getClass)
    }
}

// vim: set ts=4 sw=4 et:
