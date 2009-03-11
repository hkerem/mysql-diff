package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._

class MysqlLexical extends script.SqlLexical {
    val hexDigits = Set[Char]() ++ "0123456789abcdefABCDEF".toArray
    def hexDigit = elem("hex digit", hexDigits.contains(_))
    
    override def token: Parser[Token] =
        ( '0' ~ 'x' ~ rep1(hexDigit) ^^ { case o ~ x ~ b => NumericLit("0x" + b.mkString("")) }
        | super.token )
}

class MysqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override val lexical = new MysqlLexical

    import MysqlTableDdlStatement._
    
    def enum: Parser[MysqlEnumDataType] = "ENUM (" ~> rep1sep(stringValue, ",") <~ ")" ^^
        { case s => MysqlEnumDataType(s.map(_.value)) }
    def set: Parser[MysqlSetDataType] = "SET (" ~> rep1sep(stringValue, ",") <~ ")" ^^
        { case s => MysqlSetDataType(s.map(_.value)) }
    
    override def dataType = enum | set | super.dataType
    
    // http://dev.mysql.com/doc/refman/5.1/en/create-table.html
    override def dataTypeOption = (
        super.dataTypeOption
      | ("UNSIGNED" ^^^ MysqlUnsigned(true))
      | ("ZEROFILL" ^^^ MysqlZerofill(true))
      | ("CHARACTER" ~> "SET" ~> name ^^ (name => MysqlCharacterSet(name)))
      | ("COLLATE" ~> name ^^ (name => MysqlCollate(name)))
    )
    
    def nowValue: Parser[SqlValue] =
        // please keep spaces
        ("NOW ( )" | "CURRENT_TIMESTAMP") ^^^ NowValue
    
    override def naturalNumber: Parser[Int] = numericLit ^^
            { case x if x.startsWith("0x") => Integer.parseInt(x.substring(2), 16)
              case x => x.toInt }
    
    override def sqlValue = super.sqlValue | nowValue
    
    def autoIncrementability = "AUTO_INCREMENT" ^^^ AutoIncrement(true)
    
    def onUpdateCurrentTimestamp = "ON" ~ "UPDATE" ~ "CURRENT_TIMESTAMP" ^^^ OnUpdateCurrentTimestamp(true)
    
    override def columnProperty =
        super.columnProperty | autoIncrementability | onUpdateCurrentTimestamp
    
    def tableDefaultCharset: Parser[TableOption] =
        opt("DEFAULT") ~> ("CHARSET" | ("CHARACTER SET")) ~> opt("=") ~> ident ^^
            { MysqlCharacterSetTableOption(_) } 
    def tableCollate: Parser[TableOption] =
        "COLLATE" ~> opt("=") ~> ident ^^ { MysqlCollateTableOption(_) }
    
    def tableEngine: Parser[TableOption] =
        ("ENGINE" | "TYPE") ~> opt("=") ~> ident ^^ { MysqlEngineTableOption(_) }
     
    override def ukModel: Parser[UniqueKeyModel] =
        (constraint <~ "UNIQUE" <~ opt("INDEX" | "KEY")) ~ opt(name) ~ nameList ^^
            { case n1 ~ n2 ~ cs =>
                if (n1.isDefined && n2.isDefined)
                    throw new MysqlDiffException("UNIQUE KEY name specified twice")
                model.UniqueKeyModel(n1.orElse(n2), cs) }
    
    override def fk: Parser[TableDdlStatement.Entry] =
        (constraint <~ "FOREIGN KEY") ~ opt(name) ~ nameList ~ references ^^
            { case cn ~ in ~ lcs ~ r =>
                    MysqlForeignKey(
                            ForeignKeyModel(cn, lcs, r.table, r.columns, r.updatePolicy, r.deletePolicy),
                            in) }

   
    override def tableOption: Parser[TableOption] = (
        tableEngine
      | tableDefaultCharset
      | tableCollate
    )
    
    def convertToCharacterSet =
        "CONVERT TO CHARACTER SET" ~> name ~ opt("COLLATE" ~> name) ^^
            { case cs ~ coll => MysqlTableDdlStatement.ConvertToCharacterSet(cs, coll) }
    
    def changeCharacterSet =
        opt("DEFAULT") ~> "CHARACTER SET" ~> opt("=") ~> name ~ opt("COLLATE" ~> opt("=") ~> name) ^^
            { case cs ~ coll => MysqlTableDdlStatement.ChangeCharacterSet(cs, coll) }
    
    // http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
    override def alterSpecification = super.alterSpecification | convertToCharacterSet | changeCharacterSet
    
    protected def optBraces[T](parser: Parser[T]): Parser[T] =
        ("(" ~> parser <~ ")") | parser
    
    override def createTableLike: Parser[CreateTableLikeStatement] =
            ("CREATE TABLE" ~> opt("IF NOT EXISTS")) ~ name ~ optBraces("LIKE" ~> name) ^^ {
                case ifne ~ name ~ likeName => CreateTableLikeStatement(name, ifne.isDefined, likeName) }
    
    def setNames: Parser[MysqlSetNamesStatement] = "SET NAMES" ~> stringValue ^^
            { s => MysqlSetNamesStatement(s.value) }
    
    override def topLevel = setNames | super.topLevel
}

object MysqlParserCombinatorTests extends SqlParserCombinatorTests(MysqlContext) {
    val context = MysqlContext
    
    import context._
    import sqlParserCombinator._
    
    import MysqlTableDdlStatement._
    import TableDdlStatement._
    
    "parse dataTypeOption" in {
        parse(dataTypeOption)("UNSIGNED") must_== MysqlUnsigned(true)
        parse(dataTypeOption)("CHARACTER SET utf8") must_== new MysqlCharacterSet("utf8")
        parse(dataTypeOption)("COLLATE utf8bin") must_== new MysqlCollate("utf8bin")
    }
    
    "parse CREATE TABLE ... LIKE" in {
        parse(createTable)("CREATE TABLE oranges LIKE lemons") must beLike {
            case CreateTableLikeStatement("oranges", false, "lemons") => true
        }
    }
    
    "parser CONSTRAINT ... FOREIGN KEY" in {
        val t = parseCreateTableRegular(
                "CREATE TABLE servers (id INT, dc_id INT, " +
                        "CONSTRAINT dc_fk FOREIGN KEY dc_idx (dc_id) REFERENCES datacenters(id))")
        val fks = t.entries.filter { case _: MysqlForeignKey => true; case _ => false }
        fks must haveSize(1)
        fks.first must beLike {
            case MysqlForeignKey(
                    ForeignKeyModel(Some("dc_fk"), Seq("dc_id"), "datacenters", Seq("id"), None, None),
                    Some("dc_idx"))
                => true
        }
    }
    
    "named UNIQUE" in {
        val t = parseCreateTableRegular(
            "CREATE TABLE users (login VARCHAR(10), UNIQUE KEY login_key(login))")
        t.uniqueKeys must haveSize(1)
        t.uniqueKeys.first must beLike {
            case UniqueKey(UniqueKeyModel(Some("login_key"), Seq("login"))) => true
        }
    }
    
    "enum" in {
        parse(enum)("ENUM('week', 'month')") must beLike {
            case MysqlEnumDataType(Seq("week", "month")) => true }
        parse(createTable)("CREATE TABLE we (a ENUM('aa', 'bb'))")
    }
    
    "hex number" in {
        parse(numberValue)("0x100") must_== NumberValue(0x100)
        parse(numberValue)("0xabCDE") must_== NumberValue(0xabcde)
    }
}

// vim: set ts=4 sw=4 et:
