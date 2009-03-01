package ru.yandex.mysqlDiff.script.parser

import scala.collection.mutable.ArrayBuffer

import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import scalax.io._

import model._

import Implicits._

class CombinatorParserException(msg: String, cause: Throwable) extends Exception(msg, cause) {
    def this(msg: String) = this(msg, null)
}

class SqlLexical extends StdLexical {
    // ?
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    override def letter = elem("letter", x => x.isLetter || x == '_' || x == '%') // ?
    
    override def token: Parser[Token] =
        ( '`' ~ rep( chrExcept('`', '\n', EofCh) ) ~ '`' ^^ { case '`' ~ chars ~ '`' => Identifier(chars mkString "") }
        | '"' ~ rep( chrExcept('"', '\n', EofCh) ) ~ '"' ^^ { case '"' ~ chars ~ '"' => Identifier(chars mkString "") }
        | super.token )
    
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
class SqlParserCombinator(context: Context) extends StandardTokenParsers {
    import context._

    override val lexical = new SqlLexical
    
    import CreateTableStatement._
    
    lexical.delimiters += ("(", ")", "=", ",", ";", "=", "!=", "-", "*") // hack
    
    def trueKeyword(chars: String): Parser[String] = {
        def itIs(elem: Elem) = elem.isInstanceOf[lexical.Identifier] && elem.chars.equalsIgnoreCase(chars)
        acceptIf(itIs _)(elem => chars.toUpperCase + " expected, got " + elem.chars) ^^ ( _.chars )
    }
    
    implicit override def keyword(chars: String): Parser[String] =
        (accept(lexical.Keyword(chars)) ^^ (_.chars)) | trueKeyword(chars)
    
    // should be NullValue
    def nullValue: Parser[SqlValue] = "NULL" ^^^ NullValue
    
    def naturalNumber: Parser[Int] = numericLit ^^ { x => x.toInt }
    
    def intNumber: Parser[Int] = opt("-") ~ naturalNumber ^^ { case sign ~ value => (if (sign.isDefined) -1 else 1) * value }
    
    def numberValue: Parser[NumberValue] = intNumber ^^ { x => NumberValue(x) } // silly
    
    def stringValue: Parser[StringValue] = stringLit ^^
            { x => StringValue(x.replaceFirst("^[\"']", "").replaceFirst("[\"']$", "")) }
    
    def sqlValue: Parser[SqlValue] = nullValue | numberValue | stringValue
    
    def dataTypeOption: Parser[DataTypeOption] = failure("no data type option")
    
    def dataTypeName = name
    
    // XXX: store unsigned
    def dataType: Parser[DataType] = dataTypeName ~ opt("(" ~> naturalNumber <~ ")") ~ rep(dataTypeOption) ^^
            { case name ~ length ~ options =>
                    dataTypes.make(name.toUpperCase, length, new DataTypeOptions(options)) }
   
    def nullability: Parser[Nullability] = opt("NOT") <~ "NULL" ^^ { x => Nullability(x.isEmpty) }
    
    def defaultValue: Parser[DefaultValue] = "DEFAULT" ~> sqlValue ^^ { value => DefaultValue(value) }
    
    def uniqueAttr = "UNIQUE" ^^^ InlineUnique
    def pkAttr = "PRIMARY" ~ "KEY" ^^^ InlinePrimaryKey
    
    def referencesAttr: Parser[InlineReferences] = ("REFERENCES" ~> name <~ "(") ~ name <~ ")" ^^
        { case t ~ c => InlineReferences(t, c) }
    
    def columnProperty: Parser[ColumnProperty] = nullability | defaultValue
    
    def columnAttr: Parser[ColumnPropertyDecl] =
        columnProperty ^^ { p => ModelColumnProperty(p) } | uniqueAttr | pkAttr | referencesAttr
    
    def column: Parser[Column] = name ~ dataType ~ rep(columnAttr) ^^
            { case name ~ dataType ~ attrs => Column(name, dataType, attrs) }
    
    def name: Parser[String] = ident
    
    def nameList: Parser[Seq[String]] = "(" ~> repsep(name, ",") <~ ")"
    
    def binaryOp = "+" | "-"
    
    // XXX: unused yet
    def selectBinary: Parser[SelectExpr] = selectExpr ~ binaryOp ~ selectExpr ^^
        { case a ~ op ~ b => new SelectBinary(a, op, b) }
    
    def selectExpr: Parser[SelectExpr] = (
        "*" ^^^ SelectStar
      | name ^^ { case name => new SelectName(name) }
      | sqlValue ^^ { case value => new SelectValue(value) }
    )
    
    /** value op value to boolean */
    def booleanOp: Parser[String] = "=" | "!=" | "LIKE"
    
    def llBinaryCondition: Parser[SelectExpr] = selectExpr ~ booleanOp ~ selectExpr ^^ {
        case a ~ x ~ b => SelectBinary(a, x, b)
    }
    
    /** Lowest */
    def llCondition: Parser[SelectExpr] = 
        ("(" ~> selectCondition <~ ")") | llBinaryCondition
    
    def andCondition: Parser[SelectExpr] = rep1sep(llCondition, "AND") ^^ {
        case ands => ands.reduceLeft {
            (c1: SelectExpr, c2: SelectExpr) => SelectBinary(c1, "AND", c2)
        }
    }
    
    def orCondition: Parser[SelectExpr] = rep1sep(andCondition, "OR") ^^ {
        case ors => ors.reduceLeft {
            (c1: SelectExpr, c2: SelectExpr) => SelectBinary(c1, "OR", c2)
        }
    }
    
    def selectCondition: Parser[SelectExpr] = orCondition
    
    def from: Parser[Seq[String]] = "FROM" ~> rep1sep(name, ",")
    
    def select: Parser[SelectStatement] = ("SELECT" ~> rep1sep(selectExpr, ",")) ~ from ~ opt("WHERE" ~> selectCondition) ^^
        { case exprs ~ names ~ where => SelectStatement(exprs, names, where) }
    
    /** True iff unique */
    def indexUniquality: Parser[Boolean] =
        (opt("UNIQUE") <~ ("INDEX" | "KEY") ^^ { x => x.isDefined }) |
        ("UNIQUE" ^^^ true)
    
    // XXX: length ignored
    def indexColName: Parser[String] = name <~ opt("(" ~> naturalNumber <~ ")") <~ opt("ASC" | "DESC")
    
    def indexColNameList: Parser[Seq[String]] = "(" ~> repsep(indexColName, ",") <~ ")"
    
    def index: Parser[Index] = indexUniquality ~ opt(name) ~ indexColNameList ^^
            { case unique ~ name ~ columnNames => Index(model.IndexModel(name, columnNames, unique)) }
    
    def fk: Parser[ForeignKey] = ("FOREIGN" ~> "KEY" ~> opt(name)) ~ nameList ~ ("REFERENCES" ~> name) ~ nameList ^^
            { case k ~ lcs ~ et ~ ecs => ForeignKey(ForeignKeyModel(k, lcs, et, ecs)) }
    
    def pk: Parser[PrimaryKey] = "PRIMARY" ~> "KEY" ~> opt(name) ~ indexColNameList ^^
        { case name ~ nameList => PrimaryKey(PrimaryKeyModel(name, nameList)) }
    
    def tableEntry: Parser[Entry] = pk | fk | index | column
    
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
    
    def createTable = "CREATE" ~ "TABLE" ~> opt(ifNotExists) ~ name ~
            ("(" ~> repsep(tableEntry, ",") <~ ")") ~ rep(tableOption) ^^
                    { case ifne ~ name ~ entries ~ options => CreateTableStatement(name, ifne.isDefined, entries, options) }
    
    def createView = "CREATE" ~ "VIEW" ~> opt(ifNotExists) ~> name ~ ("AS" ~> select) ^^
        { case name ~ select => CreateViewStatement(name, select) }
    
    def dropTable = "DROP" ~ "TABLE" ~> name ^^ { name => DropTableStatement(name) }
    
    def dropView = "DROP" ~ "VIEW" ~> name ^^ { name => DropViewStatement(name) }
    
    def ddlStmt: Parser[DdlStatement] = createTable | createView | dropTable | dropView
    
    /// DML
    
    def insertDataRow: Parser[Seq[SqlValue]] = "(" ~> rep1sep(sqlValue, ",") <~ ")"
    
    def insert: Parser[InsertStatement] =
        (("INSERT" ~> opt("IGNORE") <~ "INTO") ~ name ~ opt(nameList) <~ "VALUES") ~ rep1sep(insertDataRow, ",") ^^
            { case ignore ~ name ~ columns ~ data => new InsertStatement(name, ignore.isDefined, columns, data) }
    
    def topLevel: Parser[Any] = ddlStmt | insert
    
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
    
    def parseCreateView(text: String) =
        parse(createView)(text)
    
    def parseColumn(text: String) =
        parse(column)(text)
    
    def parseValue(text: String) = {
        require(text != null && text.length >= 0, "value must be not empty string")
        try {
            parse(sqlValue)(text)
        } catch {
            case e: CombinatorParserException =>
                throw new CombinatorParserException("cannot parse '" + text + "' as SQL value", e)
        }
    }
   
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

class SqlParserCombinatorTests(context: Context) extends org.specs.Specification {
    import context._
    import sqlParserCombinator._
    
    import CreateTableStatement._
    
    "parseScript" in {
        parse("CREATE TABLE a (id INT); CREATE TABLE b (name VARCHAR(10))")
    }
    
    "parseCreateTable simple" in {
        parseCreateTable("CREATE TABLE a (id INT)") must beLike {
            case t @ CreateTableStatement("a", _, _, _) =>
                t.columns.length must_== 1
                //t.columns must beLike { case s: Seq[_] => s.length == 1 }
                t.column("id") must beLike { case Column("id", _, attrs) if attrs.isEmpty => true }
                true
        }
    }
    
    "parse CREATE VIEW" in {
        val v = parseCreateView("CREATE VIEW users_v AS SELECT * FROM users WHERE id != 0")
        v.name must_== "users_v"
        v.select.expr must beLike { case Seq(SelectStar) => true }
    }
    
    "parse DROP VIEW" in {
        parse(dropView)("DROP VIEW users_v") must beLike { case DropViewStatement("users_v") => true }
    }
    
    "parse DROP TABLE" in {
        parse(dropTable)("DROP TABLE users") must beLike { case DropTableStatement("users") => true }
    }
    
    "parse SELECT" in {
        val s = parse(select)("SELECT * FROM users WHERE login = 'colonel'")
        s must beLike {
            case SelectStatement(Seq(SelectStar), Seq("users"), _) => true
        }
        s.condition.get must_== SelectBinary(SelectName("login"), "=", SelectValue(StringValue("colonel")))
    }
    
    "parse selectExpr" in {
        parse(selectExpr)("*") must_== SelectStar
        parse(selectExpr)("users") must_== new SelectName("users")
        parse(selectExpr)("12") must_== new SelectValue(new NumberValue(12))
        parse(selectExpr)("'aa'") must_== new SelectValue(new StringValue("aa"))
    }
    
    "parse selectCondition" in {
        parse(selectCondition)("1 = 1 AND name Like 'vas%'") must beLike {
            case SelectBinary(l, "AND", r) =>
                l must beLike {
                    case SelectBinary(SelectValue(NumberValue(1)), "=", SelectValue(NumberValue(1))) => true
                }
                r must beLike {
                    case SelectBinary(SelectName("name"), "Like", SelectValue(StringValue("vas%"))) => true
                }
        }
    }
    
    "parse selectCondition AND higher then OR" in {
        val or = parse(selectCondition)("1 = 2 OR 2 = 3 AND 4 = 5")
        or must beLike { case SelectBinary(a, "OR", b) => true }
        val SelectBinary(a, "OR", b) = or
        a must beLike {
            case SelectBinary(SelectValue(NumberValue(1)), "=", SelectValue(NumberValue(2))) => true
        }
        b must beLike {
            case SelectBinary(c, "AND", d) =>
                c must beLike { case SelectBinary(_, "=", _) => true }
                d must beLike { case SelectBinary(_, "=", _) => true }
        }
    }
    
    "parse selectCondition braces" in {
        val and = parse(selectCondition)("1 = 2 AND (2 = 3 OR 4 = 5)")
        and must beLike { case SelectBinary(a, "AND", b) => true }
        val SelectBinary(a, "AND", b) = and
        a must beLike {
            case SelectBinary(SelectValue(NumberValue(1)), "=", SelectValue(NumberValue(2))) => true
        }
        b must beLike {
            case SelectBinary(c, "OR", d) =>
                c must beLike { case SelectBinary(_, "=", _) => true }
                d must beLike { case SelectBinary(_, "=", _) => true }
        }
    }
    
    "parse selectCondition first AND computed before second" in {
        val and = parse(selectCondition)("1 = 2 AND 3 = 4 AND 5 = 6")
        and must beLike {
            case SelectBinary(SelectBinary(_, "AND", _), "AND", _) => true
        }
        val SelectBinary(SelectBinary(a, "AND", b), "AND", c) = and
        a must beLike { case SelectBinary(SelectValue(NumberValue(1)), "=", SelectValue(NumberValue(2))) => true }
        b must beLike { case SelectBinary(SelectValue(NumberValue(3)), "=", SelectValue(NumberValue(4))) => true }
        c must beLike { case SelectBinary(SelectValue(NumberValue(5)), "=", SelectValue(NumberValue(6))) => true }
    }
    
    "quotes in identifiers" in {
        val t = parseCreateTable("""CREATE TABLE `a` (`id` INT, "login" VARCHAR(100))""")
        t.name must_== "a"
        t.columns must beLike { case Seq(Column("id", _, _), Column("login", _, _)) => true }
    }
    
    "parseColumn default value" in {
        val column = parseColumn("friend_files_count INT NOT NULL DEFAULT 17")
        column.name must_== "friend_files_count"
        //column.dataType must_== dataTypes.int
        column.dataType.name must_== "INT"
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
        //t.column("id").dataType must_== dataTypes.int
        t.column("id").dataType.name must_== "INT"
    }
    
    "parse references from column" in {
        val t = parseCreateTable(
                "CREATE TABLE a (id INT PRIMARY KEY, city_id INT REFERENCES city(id), name VARCHAR(10) UNIQUE)")
        t.column("city_id").properties must beSameSeqAs(List(InlineReferences("city", "id")))
        t.column("id").properties must beSameSeqAs(List(InlinePrimaryKey))
        t.column("name").properties must beSameSeqAs(List(InlineUnique))
    }
    
    "parse indexes" in {
        val t = parseCreateTable("CREATE TABLE a(id INT, UNIQUE(a), INDEX i2(b, c), UNIQUE KEY(d, e))")
        t.indexes must haveSize(3)
        t.indexes(0).index must beLike { case IndexModel(None, Seq("a"), true) => true; case _ => false }
        t.indexes(1).index must beLike { case IndexModel(Some("i2"), Seq("b", "c"), false) => true; case _ => false }
        t.indexes(2).index must beLike { case IndexModel(None, Seq("d", "e"), true) => true; case _ => false }
    }
    
    "parse FK" in {
        val t = parseCreateTable(
                "CREATE TABLE a (id INT, " +
                "FOREIGN KEY (x, y) REFERENCES b (x1, y1), " +
                "FOREIGN KEY fk1 (z) REFERENCES c (z1))")
        t.fks must haveSize(2)
        t.fks(0).fk must beLike {
            case ForeignKeyModel(None, Seq("x", "y"), "b", Seq("x1", "y1")) => true; case _ => false }
        t.fks(1).fk must beLike {
            case ForeignKeyModel(Some("fk1"), Seq("z"), "c", Seq("z1")) => true; case _ => false }
    }
    
    "parseValue -1" in {
        parseValue("-1") must_== NumberValue(-1)
    }
    
}

object SqlParserCombinatorTests extends SqlParserCombinatorTests(Environment.defaultContext)

// XXX: make class depending on context
class Parser(context: Context) {
    import context._

    def parse(text: String): Script = {
        new Script(sqlParserCombinator.parse(text).map(_.asInstanceOf[ScriptElement]))
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
