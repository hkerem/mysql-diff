package ru.yandex.mysqlDiff.script.parser

import scala.collection.mutable.ArrayBuffer

import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import scalax.io._

import model._

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

object SqlParserCombinator extends StandardTokenParsers {
    override val lexical = new SqlLexical
    
    import CreateTableStatement._
    
    lexical.delimiters += ("(", ")", "=", ",", ";", "=")
    
    lexical.reserved += ("CREATE", "TABLE", "VIEW", "IF", "NOT", "NULL", "EXISTS",
            "AS", "SELECT", "UNIQUE", "KEY", "INDEX", "PRIMARY", "DEFAULT", "NOW", "AUTO_INCREMENT")
   
    // should be NullValue
    def nullValue: Parser[SqlValue] = "NULL" ^^ { x => NullValue }
    
    def numberValue: Parser[NumberValue] = numericLit ^^ { x => NumberValue(x.toInt) } // silly
    
    def stringValue: Parser[StringValue] = stringLit ^^
            { x => StringValue(x.replaceFirst("^[\"']", "").replaceFirst("[\"']$", "")) }
    
    def nowValue: Parser[SqlValue] = "NOW" ~ "(" ~ ")" ^^ { x => NowValue }
    
    def sqlValue: Parser[SqlValue] = nullValue | numberValue | stringValue | nowValue
    
    def dataType: Parser[DataType] = name ~ opt("(" ~> numericLit <~ ")") ^^
            { case name ~ length => DataType(name, length.map(_.toInt)) }
   
    def nullability: Parser[Nullable] = opt("NOT") <~ "NULL" ^^ { x => Nullable(x.isEmpty) }
    
    def defaultValue: Parser[DefaultValue] = "DEFAULT" ~> sqlValue ^^ { value => DefaultValue(value) }
    
    // Parser[AutoIncrement.type] does not work
    def autoIncrementability: Parser[ColumnProperty] =
        "AUTO_INCREMENT" ^^ { x => AutoIncrement }
    
    def columnAttr = nullability | defaultValue | autoIncrementability
    
    def column: Parser[Column] = name ~ dataType ~ rep(columnAttr) ^^
            { case name ~ dataType ~ attrs => Column(name, dataType, attrs) }
    
    def name: Parser[String] = ident
    
    def nameList: Parser[Seq[String]] = "(" ~> repsep(name, ",") <~ ")"
    
    def trash: Parser[Any] = name | "=" | "DEFAULT"
    
    //def select: Parser[Any] = "SELECT" ~> rep(not(";" | lexical.EOF))
    def select: Parser[Any] = "SELECT" ~> rep(name)
    
    // XXX: why need braces?
    def index: Parser[Index] = (opt("UNIQUE") <~ ("INDEX" | "KEY")) ~ opt(name) ~ nameList ^^
            { case unique ~ name ~ columnNames => Index(IndexModel(name, columnNames, unique.isDefined)) }
    
    def pk: Parser[PrimaryKey] =
        "PRIMARY" ~> "KEY" ~> nameList ^^ { nameList => PrimaryKey(model.PrimaryKey(None, nameList)) }
    
    def tableEntry: Parser[Entry] = column | index | pk
    
    def ifNotExists: Parser[Any] = "IF" ~ "NOT" ~ "EXISTS"
    
    //def tableOptions: Parser[Any] = 
    
    // XXX: parse table options
    def createTable: Parser[CreateTableStatement2] = "CREATE" ~> "TABLE" ~> opt(ifNotExists) ~ name ~
            ("(" ~> repsep(tableEntry, ",") <~ ")") ~ rep(trash) ^^
                    { case ifne ~ name ~ entries ~ _ => CreateTableStatement2(name, ifne.isDefined, entries) }
    
    def createView: Parser[Any] = "CREATE" ~ "VIEW" ~ opt(ifNotExists) ~ name ~ opt(nameList) ~ "AS" ~ select
    
    def topLevel: Parser[Any] = createView | createTable
    
    def script: Parser[Seq[Any]] = repsep(topLevel, ";") <~ opt(";") ~ lexical.EOF
    
    def parse(text: String): Seq[Any] = {
        val tokens = new lexical.Scanner(text)
        val Success(result, _) = phrase(script)(tokens)
        result
    }
    
    def main(args: Array[String]) {
        val text =
            if (args.length == 1) {
                args(0)
            } else {
                ReaderResource.apply(args(1)).slurp()
            }
        println(parse(text))
    }
}

object Parser {
    def parse(text: String): Script = {
        val c = SqlParserCombinator
        new Script(SqlParserCombinator.parse(text).map(_.asInstanceOf[ScriptElement]))
    }
    
    def main(args: Array[String]) {
        val text =
            if (args.length == 1) {
                args(0)
            } else {
                ReaderResource.apply(args(1)).slurp()
            }
        val script = parse(text)
        print(ScriptSerializer.serialize(script.stmts, ScriptSerializer.Options.multiline))
    }
}

// vim: set ts=4 sw=4 et:
