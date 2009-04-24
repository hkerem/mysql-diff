package ru.yandex.mysqlDiff.script

import scala.collection.mutable.ArrayBuffer

import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import model._

import Implicits._

import scalax.io._

class CombinatorParserException(msg: String, cause: Throwable) extends Exception(msg, cause) {
    def this(msg: String) = this(msg, null)
}

class SqlLexical extends StdLexical {
    // ?
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    // XXX: % is a hack for Yandex.Video
    override def letter = elem("letter", x => x.isLetter || x == '_' || x == '%') // ?
    
    protected def join(a: Any): String = a match {
        case a ~ b => join(a) + join(b)
        case x: Seq[_] => x.map(join _).mkString("")
        case Some(x) => join(x)
        case None => ""
        case c: Char => c.toString
    }
    
    /** SQL-specific hacks */
    override def token: Parser[Token] =
        ( '"' ~ rep( chrExcept('"', '\n', EofCh) ) ~ '"' ^^ { case '"' ~ chars ~ '"' => Identifier(chars mkString "") }
        | opt('-') ~ digit ~ rep(digit) ~ opt('.' ~ digit ~ rep(digit)) ^^ { case x => NumericLit(join(x)) }
        | super.token )
    
    //override def token: Parser[Token] =
    //    letter ~ rep(letter | digit)
    
    /**
     * SQL comments are whitespaces.
     */
    override def whitespace: Parser[Any] = rep(
        whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ comment
      | ('-' ~ '-' ~ rep(chrExcept(EofCh, '\n')))
    )
    
    // All operators must be listed here
    delimiters += ("(", ")", "=", ",", ";", "=", "!=", "-", "*")
    
}

/*
 * Note to readers: ~ operator has higher priority then ~> or <~
 */
class SqlParserCombinator(context: Context) extends StandardTokenParsers {
    import context._

    override val lexical = new SqlLexical
    
    import TableDdlStatement._
    
    def trueKeyword(chars: String): Parser[String] = {
        def itIs(elem: Elem) = elem.isInstanceOf[lexical.Identifier] && elem.chars.equalsIgnoreCase(chars)
        acceptIf(itIs _)(elem => chars.toUpperCase + " expected, got " + elem.chars) ^^ ( _.chars )
    }
    
    private def singleWordKeyword(chars: String): Parser[String] =
        // avoiding keywords declaration
        (accept(lexical.Keyword(chars)) ^^ (_.chars)) | trueKeyword(chars)
    
    private def keywordSeq(kws: Seq[String]): Parser[String] = {
        kws match {
            case Seq(chars) => singleWordKeyword(chars)
            case Seq(first, rest @ _*) =>
                singleWordKeyword(first) ~ keywordSeq(rest) ^^ { case a ~ b => a + " " + b } // lies
        }
    }
    
    implicit override def keyword(chars: String): Parser[String] =
        // trick: "CREATE TABLE" is treated almost as "CREATE" ~ "TABLE"
        keywordSeq(chars.split(" "))
    
    def stringConstant: Parser[String] =
        stringLit ^^ { x => x.replaceFirst("^[\"']", "").replaceFirst("[\"']$", "") }
    
    // should be NullValue
    def nullValue: Parser[SqlValue] = "NULL" ^^^ NullValue
    
    def naturalNumber: Parser[Int] = numericLit ^^ { x => x.toInt }
    
    def bigDecimal: Parser[BigDecimal] = numericLit ^^ { x => BigDecimal(x) }
    
    def numberValue: Parser[NumberValue] = bigDecimal ^^ { x => NumberValue(x) }
    
    def stringValue: Parser[StringValue] = stringConstant ^^ { x => StringValue(x) }
    
    def booleanValue: Parser[BooleanValue] = ("TRUE" ^^^ BooleanValue(true)) | ("FALSE" ^^^ BooleanValue(false))
    
    def temporalValue: Parser[TemporalValue] =
        ( ("TIMESTAMP" ~ opt("WITHOUT TIME ZONE") ~> stringConstant) ^^ { x => new TimestampValue(x) }
        | ("TIMESTAMP WITH TIME ZONE" ~> stringConstant) ^^ { x => new TimestampWithTimeZoneValue(x) }
        | ("TIME" ~ opt("WITHOUT TIME ZONE") ~> stringConstant) ^^ { x => new TimeValue(x) }
        | ("TIME WITH TIME ZONE" ~> stringConstant) ^^ { x => new TimeWithTimeZoneValue(x) }
        | ("DATE" ~> stringConstant) ^^ { x => new DateValue(x) }
        )
    
    def sqlValue: Parser[SqlValue] = nullValue | numberValue | stringValue | booleanValue | temporalValue
    
    def dataTypeName = name
    
    def dataType: Parser[DataType] = dataTypeName ~ opt("(" ~> naturalNumber <~ ")") ^^
            { case name ~ length => dataTypes.make(name.toUpperCase, length) }
   
    def nullability: Parser[Nullability] = opt("NOT") <~ "NULL" ^^ { x => Nullability(x.isEmpty) }
    
    def defaultValue: Parser[DefaultValue] = "DEFAULT" ~> sqlValue ^^ { value => DefaultValue(value) }
    
    def uniqueAttr = "UNIQUE" ^^^ TableDdlStatement.InlineUnique
    def pkAttr = "PRIMARY" ~ "KEY" ^^^ TableDdlStatement.InlinePrimaryKey
    
    def importedKeyPolicy: Parser[ImportedKeyRule] =
        ( ("NO ACTION" ^^^ ImportedKeyNoAction)
        | ("RESTRICT" ^^^ ImportedKeyNoAction)
        | ("CASCADE" ^^^ ImportedKeyCascade)
        | ("SET NULL" ^^^ ImportedKeySetNull)
        | ("SET DEFAULT" ^^^ ImportedKeySetNull)
        )
    
    private abstract case class OnSomething(p: ImportedKeyRule)
    private case class OnDelete(override val p: ImportedKeyRule) extends OnSomething(p)
    private case class OnUpdate(override val p: ImportedKeyRule) extends OnSomething(p)
    
    private def onSomething: Parser[OnSomething] =
        ( ( "ON UPDATE" ~> importedKeyPolicy ^^ { p => OnUpdate(p) } )
        | ( "ON DELETE" ~> importedKeyPolicy ^^ { p => OnDelete(p) } )
        )
    
    def references = "REFERENCES" ~> name ~ nameList ~ rep(onSomething) ^^
        { case t ~ cs ~ onss =>
            TableDdlStatement.References(t, cs,
                    onss.find(_.isInstanceOf[OnUpdate]).map(_.p).orElse(Some(ImportedKeyNoAction)),
                    onss.find(_.isInstanceOf[OnDelete]).map(_.p).orElse(Some(ImportedKeyNoAction))) }

    
    def referencesAttr = references ^^ { r => TableDdlStatement.InlineReferences(r) }
     
    def columnProperty: Parser[ColumnProperty] = nullability | defaultValue
    
    def columnAttr: Parser[TableDdlStatement.ColumnPropertyDecl] =
        columnProperty ^^ { p => TableDdlStatement.ModelColumnProperty(p) } | uniqueAttr | pkAttr | referencesAttr
    
    def columnModel: Parser[ColumnModel] = name ~ dataType ~ rep(columnProperty) ^^
            { case name ~ dataType ~ ps => ColumnModel(name, dataType, ps) }
    
    def column: Parser[TableDdlStatement.Column] = name ~ dataType ~ rep(columnAttr) ^^
            { case name ~ dataType ~ attrs => TableDdlStatement.Column(name, dataType, attrs) }
    
    def name: Parser[String] = ident
    
    def nameList: Parser[Seq[String]] = "(" ~> repsep(name, ",") <~ ")"
    
    def binaryOp = "+" | "-"
    
    // XXX: unused yet
    // XXX: copy full grammar from 2-6.34
    def selectBinary: Parser[SqlExpr] = sqlExpr ~ binaryOp ~ sqlExpr ^^
        { case a ~ op ~ b => new BinaryOpExpr(a, op, b) }
    
    def cast: Parser[CastExpr] = "CAST (" ~> sqlExpr ~ ("AS" ~> dataType <~ ")") ^^
            { case e ~ t => CastExpr(e, t) }
    
    def sqlExpr: Parser[SqlExpr] =
        ( sqlValue
        | cast
        | ("*" ^^^ SelectStar)
        | (name ^^ { case name => new NameExpr(name) })
        )
    
    /** value op value to boolean */
    def booleanOp: Parser[String] = "=" | "!=" | "LIKE"
    
    def llBinaryCondition: Parser[SqlExpr] = sqlExpr ~ booleanOp ~ sqlExpr ^^ {
        case a ~ x ~ b => BinaryOpExpr(a, x, b)
    }
    
    /** Lowest */
    def llCondition: Parser[SqlExpr] = 
        ("(" ~> searchCondition <~ ")") | llBinaryCondition
    
    def andCondition: Parser[SqlExpr] = rep1sep(llCondition, "AND") ^^ {
        case ands => ands.reduceLeft {
            (c1: SqlExpr, c2: SqlExpr) => BinaryOpExpr(c1, "AND", c2)
        }
    }
    
    def orCondition: Parser[SqlExpr] = rep1sep(andCondition, "OR") ^^ {
        case ors => ors.reduceLeft {
            (c1: SqlExpr, c2: SqlExpr) => BinaryOpExpr(c1, "OR", c2)
        }
    }
    
    def searchCondition: Parser[SqlExpr] = orCondition
    
    def from: Parser[Seq[String]] = "FROM" ~> rep1sep(name, ",")
    
    def select: Parser[SelectStatement] = ("SELECT" ~> rep1sep(sqlExpr, ",")) ~ from ~ opt("WHERE" ~> searchCondition) ^^
        { case exprs ~ names ~ where => SelectStatement(exprs, names, where) }
    
    // XXX: length ignored
    def indexColName: Parser[String] = name <~ opt("(" ~> naturalNumber <~ ")") <~ opt("ASC" | "DESC")
    
    def indexColNameList: Parser[Seq[String]] = "(" ~> repsep(indexColName, ",") <~ ")"
    
    def indexModel: Parser[IndexModel] = failure("no inline index in ANSI SQL")
    
    def constraint: Parser[Option[String]] = opt("CONSTRAINT" ~> name)
    
    def ukModel: Parser[UniqueKeyModel] =
        (constraint <~ "UNIQUE" <~ opt("INDEX" | "KEY")) ~ nameList ^^
            { case n ~ cs => model.UniqueKeyModel(n, cs) }
    
    def fkModel: Parser[ForeignKeyModel] =
        (constraint <~ "FOREIGN KEY") ~ nameList ~ references ^^
            { case cn ~ lcs ~ r =>
                    ForeignKeyModel(cn, lcs, r.table, r.columns, r.updateRule, r.deleteRule) }
    
    /** Must not be <code>Parser[TableDdlStatement.ForeignKey]</code> to be overrideable */
    def fk: Parser[TableDdlStatement.Extra] = fkModel ^^ { fk => TableDdlStatement.ForeignKey(fk) }
    
    def pkModel: Parser[PrimaryKeyModel] =
        (constraint <~ "PRIMARY KEY") ~ indexColNameList ^^
            { case name ~ nameList => PrimaryKeyModel(name, nameList) }
    
    def ifNotExists: Parser[Any] = "IF NOT EXISTS"
    
    def ifExists: Parser[Any] = "IF EXISTS"
    
    def tableOption: Parser[TableOption] = failure("no table options in standard parser")
    
    // <table scope>
    def tableScope: Parser[Any] = ("GLOBLAL" | "LOCAL") ~ "TEMPORARY"
    
    // <like clause>
    def likeClause: Parser[LikeClause] = "LIKE" ~> name ^^ { LikeClause(_) }
    
    // <table element>
    def tableElement: Parser[TableElement] =
        ( likeClause
        | (pkModel ^^ { p => TableDdlStatement.PrimaryKey(p) })
        | fk
        | (ukModel ^^ { f => TableDdlStatement.UniqueKey(f) })
        | (indexModel ^^ { i => TableDdlStatement.Index(i) })
        | column
        )
    
    
    // <table element list>
    def tableElementList: Parser[TableElementList] =
        "(" ~> rep1sep(tableElement, ",") <~ ")" ^^ { TableElementList(_) }
    
    // <table contents source>
    def tableContentsSource: Parser[TableContentsSource] = tableElementList
    
    // <table definition>
    def createTable =
        "CREATE" ~> opt(tableScope) ~> "TABLE" ~> opt(ifNotExists) ~ name ~
            tableContentsSource ~ rep(tableOption) ^^
                { case i ~ n ~ s ~ o => CreateTableStatement(n, i.isDefined, s, o) }
    
    def createView = "CREATE VIEW" ~> opt(ifNotExists) ~> name ~ ("AS" ~> select) ^^
        { case name ~ select => CreateViewStatement(name, select) }
    
    def dropTable =
        "DROP TABLE" ~> opt(ifExists) ~ name ^^
                { case ifExists ~ name => DropTableStatement(name, ifExists.isDefined) }
    
    def dropView = "DROP VIEW" ~> name ^^ { name => DropViewStatement(name) }
    
    def columnPosition: Parser[TableDdlStatement.ColumnPosition] =
        ( "FIRST" ^^^ TableDdlStatement.ColumnFirst
        | "AFTER" ~> name ^^ { TableDdlStatement.ColumnAfter(_) }
        )
    
    // XXX: add multiple column
    // XXX: use FIRST, AFTER
    def addColumn = "ADD" ~> opt("COLUMN") ~> column ~ opt(columnPosition) ^^
            { case c ~ p => TableDdlStatement.AddColumn(c, p) }
    
    def addIndex = "ADD" ~> indexModel ^^ { ind => TableDdlStatement.AddIndex(ind) }
    
    def addUk = "ADD" ~> ukModel ^^ { uk => TableDdlStatement.AddUniqueKey(uk) }
    
    def addPk = "ADD" ~> pkModel ^^ { pk => TableDdlStatement.AddPrimaryKey(pk) }
    
    def addFk = "ADD" ~> fk ^^ { fk => TableDdlStatement.AddExtra(fk) }
    
    def alterColumn = "ALTER" ~> opt("COLUMN") ~> name ~
        (("SET DEFAULT" ~> sqlValue ^^ { x => Some(x) }) | ("DROP DEFAULT" ^^^ None)) ^^
            { case n ~ v => TableDdlStatement.AlterColumnSetDefault(n, v) }
    
    // XXX: use column position
    def changeColumn = "CHANGE" ~> opt("COLUMN") ~> name ~ columnModel ~ opt(columnPosition) ^^
        { case n ~ c ~ p => TableDdlStatement.ChangeColumn(n, c, p) }
    
    def modifyColumn = "MODIFY" ~> opt("COLUMN") ~> columnModel ~ opt(columnPosition) ^^
        { case c ~ p => TableDdlStatement.ModifyColumn(c, p) }
    
    def dropColumn = "DROP" ~> opt("COLUMN") ~> name ^^ { n => TableDdlStatement.DropColumn(n) }
    
    def dropPk = "DROP PRIMARY KEY" ^^^ TableDdlStatement.DropPrimaryKey
    
    def dropKey = "DROP" ~> ("INDEX" | "KEY") ~> name ^^ { n => TableDdlStatement.DropIndex(n) }
    
    def dropFk = "DROP FOREIGN KEY" ~> name ^^ { n => TableDdlStatement.DropForeignKey(n) }
    
    def disableEnableKeys =
        ("DISABLE KEYS" ^^^ TableDdlStatement.DisableKeys) |
        ("ENABLE KEYS" ^^^ TableDdlStatement.EnableKeys)
    
    def alterTableRename = "RENAME" ~> opt("TO") ~> name ^^ { n => TableDdlStatement.Rename(n) }
    
    def alterTableOrderBy = "ORDER BY" ~> rep1sep(name, ",") ^^ { l => TableDdlStatement.OrderBy(l) }
    
    def alterTableOption = tableOption ^^ { o => TableDdlStatement.ChangeTableOption(o) }
    
    def alterSpecification: Parser[TableDdlStatement.Operation] = addIndex | addPk | addUk | addFk | addColumn |
        alterColumn | changeColumn | modifyColumn |
        dropPk | dropKey | dropFk | dropColumn |
        disableEnableKeys | alterTableRename | alterTableOrderBy | alterTableOption
    
    // http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
    def alterTable: Parser[AlterTableStatement] = "ALTER TABLE" ~> name ~ rep1sep(alterSpecification, ",") ^^
        { case name ~ ops => AlterTableStatement(name, ops) }
    
    def ddlStmt: Parser[DdlStatement] = createTable | createView | dropTable | dropView | alterTable
    
    /// DML
    
    def insertDataRow: Parser[Seq[SqlExpr]] = "(" ~> rep1sep(sqlExpr, ",") <~ ")"
    
    def insert: Parser[InsertStatement] =
        (("INSERT" ~> opt("IGNORE") <~ "INTO") ~ name ~ opt(nameList) <~ "VALUES") ~ rep1sep(insertDataRow, ",") ^^
            { case ignore ~ name ~ columns ~ data => new InsertStatement(name, ignore.isDefined, columns, data) }
    
    def updateSet: Parser[(String, SqlExpr)] = name ~ ("=" ~> sqlExpr) ^^ { case a ~ b => (a, b) }
    
    // update statement: searched
    def update: Parser[UpdateStatement] =
        ("UPDATE" ~> name) ~ ("SET" ~> rep1sep(updateSet, ", ")) ~ opt("WHERE" ~> searchCondition) ^^
            { case name ~ updates ~ cond => UpdateStatement(name, updates, cond) }
    
    // delete statement: searched
    def delete: Parser[DeleteStatement] =
        ("DELETE FROM" ~> name) ~ opt("WHERE" ~> searchCondition) ^^
            { case name ~ cond => DeleteStatement(name, cond) }
    
    def dmlStmt: Parser[DmlStatement] = insert | update | select | delete
    
    def topLevel: Parser[ScriptStatement] = ddlStmt | dmlStmt
    
    def script: Parser[Seq[ScriptStatement]] = rep(";") ~> repsep(topLevel, rep1(";")) <~ rep(";") ~ lexical.EOF
    
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
    
    def parseDataType(text: String) =
        parse(dataType)(text)
    
    def parseCreateTable(text: String) =
        parse(createTable)(text)
    
    def parseCreateView(text: String) =
        parse(createView)(text)
    
    def parseColumn(text: String) =
        parse(column)(text)
    
    def parseValue(text: String) = {
        require(text != null && text.trim.length > 0, "value not be a blank string")
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
    
    import TableDdlStatement._
    
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
    
    "parse CREATE TABLE ... (LIKE ...)" in {
        parse(createTable)("CREATE TABLE oranges (LIKE lemons)") must beLike {
            case CreateTableStatement("oranges", false,
                    TableElementList(Seq(LikeClause("lemons"))), Seq()) => true
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
        parse(dropTable)("DROP TABLE users") must beLike { case DropTableStatement("users", false) => true }
    }
    
    "parse SELECT" in {
        val s = parse(select)("SELECT * FROM users WHERE login = 'colonel'")
        s must beLike {
            case SelectStatement(Seq(SelectStar), Seq("users"), _) => true
        }
        s.condition.get must_== BinaryOpExpr(NameExpr("login"), "=", StringValue("colonel"))
    }
    
    "parse sqlExpr" in {
        parse(sqlExpr)("*") must_== SelectStar
        parse(sqlExpr)("users") must_== new NameExpr("users")
        parse(sqlExpr)("12") must_== new NumberValue(12)
        parse(sqlExpr)("'aa'") must_== new StringValue("aa")
    }
    
    "parse searchCondition" in {
        parse(searchCondition)("1 = 1 AND name Like 'vas%'") must beLike {
            case BinaryOpExpr(l, "AND", r) =>
                l must beLike {
                    case BinaryOpExpr(NumberValue(1), "=", NumberValue(1)) => true
                }
                r must beLike {
                    case BinaryOpExpr(NameExpr("name"), "Like", StringValue("vas%")) => true
                }
        }
    }
    
    "parse searchCondition AND higher then OR" in {
        val or = parse(searchCondition)("1 = 2 OR 2 = 3 AND 4 = 5")
        or must beLike { case BinaryOpExpr(a, "OR", b) => true }
        val BinaryOpExpr(a, "OR", b) = or
        a must beLike {
            case BinaryOpExpr(NumberValue(1), "=", NumberValue(2)) => true
        }
        b must beLike {
            case BinaryOpExpr(c, "AND", d) =>
                c must beLike { case BinaryOpExpr(_, "=", _) => true }
                d must beLike { case BinaryOpExpr(_, "=", _) => true }
        }
    }
    
    "parse searchCondition braces" in {
        val and = parse(searchCondition)("1 = 2 AND (2 = 3 OR 4 = 5)")
        and must beLike { case BinaryOpExpr(a, "AND", b) => true }
        val BinaryOpExpr(a, "AND", b) = and
        a must beLike {
            case BinaryOpExpr(NumberValue(1), "=", NumberValue(2)) => true
        }
        b must beLike {
            case BinaryOpExpr(c, "OR", d) =>
                c must beLike { case BinaryOpExpr(_, "=", _) => true }
                d must beLike { case BinaryOpExpr(_, "=", _) => true }
        }
    }
    
    "parse searchCondition first AND computed before second" in {
        val and = parse(searchCondition)("1 = 2 AND 3 = 4 AND 5 = 6")
        and must beLike {
            case BinaryOpExpr(BinaryOpExpr(_, "AND", _), "AND", _) => true
        }
        val BinaryOpExpr(BinaryOpExpr(a, "AND", b), "AND", c) = and
        a must beLike { case BinaryOpExpr(NumberValue(1), "=", NumberValue(2)) => true }
        b must beLike { case BinaryOpExpr(NumberValue(3), "=", NumberValue(4)) => true }
        c must beLike { case BinaryOpExpr(NumberValue(5), "=", NumberValue(6)) => true }
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
    
    "parse UPDATE" in {
        val updateStmt = parse(update)("UPDATE service SET s_name = name WHERE sid != 29")
        updateStmt must beLike {
            case UpdateStatement("service", Seq(("s_name", _)), _) => true }
    }
    
    "DELETE" in {
        val d = parse(delete)("DELETE FROM users WHERE 1 = 0")
        d must beLike {
            case DeleteStatement("users", Some(_)) => true }
    }
    
    "case insensitive" in {
        val t = parseCreateTable("CrEaTe TaBlE a (id InT nOt NuLl)")
        t.name must_== "a"
        t.columns must haveSize(1)
        t.columns must exist({ c: Column => c.name == "id" })
        //t.column("id").dataType must_== dataTypes.int
        t.column("id").dataType.name must_== "INT"
    }
    
    "ignores spaces" in {
        val t = parseCreateTable("  CREATE   TABLE  a (id INT NOT NULL) ")
        t.name must_== "a"
        t.columns must haveSize(1)
        t.columns must exist({ c: Column => c.name == "id" })
        //t.column("id").dataType must_== dataTypes.int
        t.column("id").dataType.name must_== "INT"
    }
    
    "parse references from column" in {
        val t = parseCreateTable(
                "CREATE TABLE a (id INT PRIMARY KEY, city_id INT REFERENCES city(id), name VARCHAR(10) UNIQUE)")
        t.column("city_id").properties must beLike {
            case Seq(InlineReferences(References("city", Seq("id"), _, _))) => true }
        t.column("id").properties must beSameSeqAs(List(InlinePrimaryKey))
        t.column("name").properties must beSameSeqAs(List(InlineUnique))
    }
    
    "parser ALTER TABLE ADD UNIQUE" in {
        import AlterTableStatement._
        val a = parse(alterTable)("ALTER TABLE convert_queue ADD UNIQUE(user_id, file_id)")
        a must beLike {
            case AlterTableStatement("convert_queue",
                    Seq(AddExtra(UniqueKey(UniqueKeyModel(None, Seq("user_id", "file_id")))))) => true }
    }
    
    "parse ALTER TABLE DROP INDEX" in {
        val a = parse(alterTable)("ALTER TABLE event DROP INDEX idx_event__query2")
        a must beLike {
            case AlterTableStatement("event",
                Seq(DropIndex("idx_event__query2"))) => true }
    }
    
    "parse ALTER TABLE ALTER COLUMN SET DEFAULT" in {
        import AlterTableStatement._
        val a = parse(alterTable)(
            "ALTER TABLE users ALTER COLUMN password DROP DEFAULT, ALTER film_count SET DEFAULT 0")
        a must beLike {
            case AlterTableStatement("users",
                    Seq(
                        AlterColumnSetDefault("password", None),
                        AlterColumnSetDefault("film_count", Some(NumberValue(0)))))
                => true }
    }
    
    "parseValue -1" in {
        parseValue("-1") must_== NumberValue(-1)
    }
    
    "parseValue 1.123" in {
        parseValue("1.123") must_== NumberValue("1.123")
    }
    
    "parseValue BOOLEAN" in {
        parseValue("TRUE") must_== BooleanValue(true)
        parseValue("FALSE") must_== BooleanValue(false)
    }
    
    "parseValue TIMSTAMP WITHOUT TIME ZONE" in {
        parseValue("TIMESTAMP WITHOUT TIME ZONE 'now'") must_== new TimestampValue("now")
    }
    
    "ignore semicolons" in {
        val stmts = parse(script)("CREATE TABLE a (id INT); ; CREATE TABLE b (id INT) ;;")
        stmts must haveSize(2)
    }
}

object SqlParserCombinatorTests extends SqlParserCombinatorTests(Environment.defaultContext)

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
        print(scriptSerializer.serialize(script.stmts, ScriptSerializer.Options.multiline))
    }
}

// vim: set ts=4 sw=4 et: