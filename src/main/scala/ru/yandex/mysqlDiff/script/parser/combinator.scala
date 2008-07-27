package ru.yandex.mysqlDiff.script.parser

import scala.collection.mutable.ArrayBuffer

import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import scalax.io._

import model._

import script.Implicits._

class CombinatorParserException(msg: String) extends Exception(msg)

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
    
}

/*
 * Note to readers: ~ operator has higher priority then ~> or <~
 */
object SqlParserCombinator extends StandardTokenParsers {
    override val lexical = new SqlLexical
    
    import CreateTableStatement._
    
    lexical.delimiters += ("(", ")", "=", ",", ";", "=")
    
    //lexical.reserved += ("CREATE", "TABLE", "VIEW", "IF", "NOT", "NULL", "EXISTS",
    //        "AS", "SELECT", "UNIQUE", "KEY", "INDEX", "PRIMARY", "DEFAULT", "NOW", "AUTO_INCREMENT")
   
    def trueKeyword(chars: String): Parser[String] = {
        def itIs(elem: Elem) = elem.isInstanceOf[lexical.Identifier] && elem.chars.equalsIgnoreCase(chars)
        acceptIf(itIs _)(elem => chars.toUpperCase + " expected, got " + elem.chars) ^^ ( _.chars )
    }
    
    // XXX: allow ignore case
    implicit override def keyword(chars: String): Parser[String] =
        (accept(lexical.Keyword(chars)) ^^ (_.chars)) | trueKeyword(chars)
    
    // should be NullValue
    def nullValue: Parser[SqlValue] = "NULL" ^^^ NullValue
    
    def numberValue: Parser[NumberValue] = numericLit ^^ { x => NumberValue(x.toInt) } // silly
    
    def stringValue: Parser[StringValue] = stringLit ^^
            { x => StringValue(x.replaceFirst("^[\"']", "").replaceFirst("[\"']$", "")) }
    
    def nowValue: Parser[SqlValue] = (("NOW" ~ "(" ~ ")") | "CURRENT_TIMESTAMP") ^^^ NowValue
    
    def sqlValue: Parser[SqlValue] = nullValue | numberValue | stringValue | nowValue
    
    def dataType: Parser[DataType] = name ~ opt("(" ~> numericLit <~ ")") ^^
            { case name ~ length => DataType(name.toUpperCase, length.map(_.toInt)) }
   
    def nullability: Parser[Nullability] = opt("NOT") <~ "NULL" ^^ { x => Nullability(x.isEmpty) }
    
    def defaultValue: Parser[DefaultValue] = "DEFAULT" ~> sqlValue ^^ { value => DefaultValue(value) }
    
    def autoIncrementability: Parser[AutoIncrement] =
        "AUTO_INCREMENT" ^^^ AutoIncrement(true)
    
    def columnAttr: Parser[ColumnPropertyDecl] =
        (nullability | defaultValue | autoIncrementability) ^^ { p => ModelColumnProperty(p) }
    
    def column: Parser[Column] = name ~ dataType ~ rep(columnAttr) ^^
            { case name ~ dataType ~ attrs => Column(name, dataType, attrs) }
    
    def name: Parser[String] = ident
    
    def nameList: Parser[Seq[String]] = "(" ~> repsep(name, ",") <~ ")"
    
    def trash: Parser[Any] = name | "=" | "DEFAULT"
    
    //def select: Parser[Any] = "SELECT" ~> rep(not(";" | lexical.EOF))
    def select: Parser[Any] = "SELECT" ~> rep(name)
    
    def index: Parser[Index] = (opt("UNIQUE") <~ ("INDEX" | "KEY")) ~ opt(name) ~ nameList ^^
            { case unique ~ name ~ columnNames => Index(IndexModel(name, columnNames, unique.isDefined)) }
    
    def pk: Parser[PrimaryKey] =
        "PRIMARY" ~> "KEY" ~> nameList ^^ { nameList => PrimaryKey(model.PrimaryKey(None, nameList)) }
    
    def tableEntry: Parser[Entry] = pk | index | column
    
    def ifNotExists: Parser[Any] = "IF" ~ "NOT" ~ "EXISTS"
    
    def tableDefaultCharset: Parser[TableOption] =
        opt("DEFAULT") ~> ("CHARSET" | ("CHARACTER" ~ "SET")) ~> opt("=") ~> ident ^^
            { TableOption("DEFAULT CHARSET", _) }
    
    def tableCollate: Parser[TableOption] =
        "COLLATE" ~> opt("=") ~> ident ^^ { TableOption("COLLATE", _) }
    
    def tableEngine: Parser[TableOption] =
        ("ENGINE" | "TYPE") ~> opt("=") ~> ident ^^ { TableOption("ENGINE", _) }
   
    def tableOption: Parser[TableOption] = (
        tableEngine
      | tableDefaultCharset
      | tableCollate
    )
    
    // XXX: parse table options
    def createTable: Parser[CreateTableStatement] = "CREATE" ~> "TABLE" ~> opt(ifNotExists) ~ name ~
            ("(" ~> repsep(tableEntry, ",") <~ ")") ~ rep(tableOption) ^^
                    { case ifne ~ name ~ entries ~ options => CreateTableStatement(name, ifne.isDefined, entries, options) }
    
    def createView: Parser[Any] = "CREATE" ~ "VIEW" ~ opt(ifNotExists) ~ name ~ opt(nameList) ~ "AS" ~ select
    
    def insertDataRow: Parser[Seq[SqlValue]] = "(" ~> rep1sep(sqlValue, ",") <~ ")"
    
    def insert: Parser[InsertStatement] =
        (("INSERT" ~> opt("IGNORE") <~ "INTO") ~ name ~ opt(nameList) <~ "VALUES") ~ rep1sep(insertDataRow, ",") ^^
            { case ignore ~ name ~ columns ~ data => new InsertStatement(name, ignore.isDefined, columns, data) }
    
    def topLevel: Parser[Any] = createView | createTable | insert
    
    def script: Parser[Seq[Any]] = repsep(topLevel, ";") <~ opt(";") ~ lexical.EOF
    
    def parse[T](parser: Parser[T])(text: String) = {
        val tokens = new lexical.Scanner(text)
        phrase(parser)(tokens) match {
            case Success(result, _) => result
            case ns: NoSuccess =>
                throw new CombinatorParserException(ns.toString) // XXX: make singleline
        }
    }
    
    def parse(text: String): Seq[Any] =
        parse(script)(text)
    
    def parseCreateTable(text: String) =
        parse(createTable)(text)
    
    def parseColumn(text: String) =
        parse(column)(text)
    
    def parseValue(text: String) =
        parse(sqlValue)(text)
   
    def parseInsert(text: String) =
        parse(insert)(text)
   
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

object SqlParserCombinatorTests extends org.specs.Specification {
    import SqlParserCombinator._
    
    "parseScript" in {
        parse("CREATE TABLE a (id INT); CREATE TABLE b (name VARCHAR(10))")
    }
    
    "parseCreateTable simple" in {
        import CreateTableStatement._
        
        parseCreateTable("CREATE TABLE a (id INT)") must beLike {
            case t @ CreateTableStatement("a", _, _, _) =>
                t.columns.length must_== 1
                //t.columns must beLike { case s: Seq[_] => s.length == 1 }
                t.column("id") must beLike { case Column("id", _, attrs) if attrs.isEmpty => true }
                true
        }
    }
    
    "parseColumn default value" in {
        val column = parseColumn("friend_files_count INT NOT NULL DEFAULT 17")
        column.name must_== "friend_files_count"
        column.dataType must_== DataType.int
        column.defaultValue must_== Some(NumberValue(17))
    }
    
    "parseColumn nullability" in {
        parseColumn("id INT NOT NULL").isNotNull must_== true
        parseColumn("id INT NULL").isNotNull must_== false
        parseColumn("id INT").isNotNull must_== false
    }
    
    "parseInsert IGNORE" in {
        val insert = parseInsert("INSERT IGNORE INTO users (id, login) VALUES (15, 'vasya')")
        insert.table must_== "users"
        insert.columns.get.toList must_== List("id", "login")
        insert.data.length must_== 1
        insert.data.first.toList must_== List(NumberValue(15), StringValue("vasya"))
    }
    
    "case insensitive" in {
        val t = parseCreateTable("CrEaTe TaBlE a (id InT nOt NuLl)")
        t.name must_== "a"
        t.columns must haveSize(1)
        t.columns must exist({ c: CreateTableStatement.Column => c.name == "id" })
        t.column("id").dataType must_== DataType.int
    }
    
    "parse references from column" in {
        //val t = parseCreateTable("CREATE TABLE a (id INT, city_id INT REFERENCES city(id))")
        // XXX
        false
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
