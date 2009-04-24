package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._

class MysqlLexical extends script.SqlLexical {
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    val hexDigits = Set[Char]() ++ "0123456789abcdefABCDEF".toArray
    def hexDigit = elem("hex digit", hexDigits.contains(_))
    
    override def token: Parser[Token] =
        ( '0' ~ 'x' ~ rep1(hexDigit) ^^ { case o ~ x ~ b => NumericLit("0x" + b.mkString("")) }
        | '`' ~ rep( chrExcept('`', '\n', EofCh) ) ~ '`' ^^ { case '`' ~ chars ~ '`' => Identifier(chars mkString "") }
        | '"' ~ rep( chrExcept('"', '\n', EofCh) ) ~ '"' ^^ { case '"' ~ chars ~ '"' => StringLit(chars mkString "") }
        | super.token )
}

class MysqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override val lexical = new MysqlLexical

    import MysqlTableDdlStatement._
    import TableDdlStatement._
    
    def enum: Parser[MysqlEnumDataType] = "ENUM (" ~> rep1sep(stringValue, ",") <~ ")" ^^
        { case s => MysqlEnumDataType(s.map(_.value)) }
    def set: Parser[MysqlSetDataType] = "SET (" ~> rep1sep(stringValue, ",") <~ ")" ^^
        { case s => MysqlSetDataType(s.map(_.value)) }
    
    def anyWord(words: Seq[String]): Parser[String] =
        words.foldLeft[Parser[String]](failure("builder"))(_ append _)
    
    def mysqlNumericDataTypeName: Parser[String] =
        anyWord(MysqlDataTypes.numericDataTypeNames) ^^ { _.toUpperCase }
    
    def mysqlNumericDataType: Parser[MysqlNumericDataType] =
        mysqlNumericDataTypeName ~ opt("(" ~> naturalNumber ~ opt("," ~> naturalNumber) <~ ")") ~
                opt("UNSIGNED") ~ opt("ZEROFILL") ^^
            { case n ~ s ~ u ~ z =>
                val (l, d) = s match {
                    case None => (None, None)
                    case Some(l ~ None) => (Some(l), None)
                    case Some(l ~ Some(d)) => (Some(l), Some(d))
                }
                MysqlNumericDataType(n, l, d, Some(u.isDefined), Some(z.isDefined)) }
    
    def mysqlCharacterDataTypeName: Parser[String] =
        anyWord(MysqlDataTypes.characterDataTypeNames) ^^ { _.toUpperCase }
    
    def mysqlCharacterDataType: Parser[MysqlCharacterDataType] =
        mysqlCharacterDataTypeName ~ opt("(" ~> naturalNumber <~ ")") ~
            opt("CHARACTER SET" ~> name) ~ opt("COLLATE" ~> name) ^^
                { case n ~ l ~ cs ~ cl => MysqlCharacterDataType(n, l, cs, cl) }
    
    def mysqlTextDataTypeName: Parser[String] =
        anyWord(MysqlDataTypes.textDataTypeNames) ^^ { _.toUpperCase }
    
    def mysqlTextDataType: Parser[MysqlTextDataType] =
        mysqlTextDataTypeName ~ opt("BINARY") ~
            opt("CHARACTER SET" ~> name) ~ opt("COLLATE" ~> name) ^^
                { case n ~ b ~ cs ~ cl => MysqlTextDataType(n, Some(b.isDefined), cs, cl) }
    
    override def dataType: Parser[DataType] =
        enum | set | mysqlNumericDataType | mysqlCharacterDataType | mysqlTextDataType | super.dataType
    
    def nowValue: Parser[SqlValue] =
        // please keep spaces
        ("NOW ( )" | "CURRENT_TIMESTAMP") ^^^ NowValue
    
    private def parseInt(x: String) =
        if (x startsWith "0x") Integer.parseInt(x.substring(2), 16)
        else x.toInt
    
    override def naturalNumber: Parser[Int] = numericLit ^^ { case x => parseInt(x) }
    
    override def bigDecimal: Parser[BigDecimal] = numericLit ^^ {
        case x if x startsWith "0x" => BigDecimal(parseInt(x))
        case x => BigDecimal(x)
    }
    
    override def sqlValue = super.sqlValue | nowValue
    
    def autoIncrementability = "AUTO_INCREMENT" ^^^ MysqlAutoIncrement(true)
    
    def onUpdateCurrentTimestamp = "ON" ~ "UPDATE" ~ "CURRENT_TIMESTAMP" ^^^ MysqlOnUpdateCurrentTimestamp(true)
    
    def columnComment = "COMMENT" ~> stringConstant ^^ { MysqlComment(_) }
    
    override def columnProperty =
        super.columnProperty | autoIncrementability | onUpdateCurrentTimestamp | columnComment
    
    override def ukModel: Parser[UniqueKeyModel] =
        (constraint <~ "UNIQUE" <~ opt("INDEX" | "KEY")) ~ opt(name) ~ nameList ^^
            { case n1 ~ n2 ~ cs =>
                if (n1.isDefined && n2.isDefined)
                    throw new MysqlDiffException("UNIQUE KEY name specified twice")
                model.UniqueKeyModel(n1.orElse(n2), cs) }
    
    // undocumented MySQL
    override def pkModel: Parser[PrimaryKeyModel] =
        (constraint <~ "PRIMARY KEY") ~ opt(name) ~ indexColNameList ^^
            { case n1 ~ n2 ~ nameList =>
                if (n1.isDefined && n2.isDefined)
                    throw new MysqlDiffException("PRIMARY KEY name specified twice")
                PrimaryKeyModel(n1.orElse(n2), nameList) }
    
    override def fk: Parser[TableDdlStatement.Extra] =
        (constraint <~ "FOREIGN KEY") ~ opt(name) ~ nameList ~ references ^^
            { case cn ~ in ~ lcs ~ r =>
                    MysqlForeignKey(
                            ForeignKeyModel(cn, lcs, r.table, r.columns, r.updateRule, r.deleteRule),
                            in) }
    
    // XXX: index type is ignored
    override def indexModel: Parser[IndexModel] =
        ("KEY" | "INDEX") ~> opt(name) ~ opt("USING" ~> ("BTREE" | "HASH" | "RTREE")) ~ indexColNameList ^^
            { case n ~ t ~ cs => model.IndexModel(n, cs) }
   
    def tableDefaultCharset: Parser[TableOption] =
        opt("DEFAULT") ~> ("CHARSET" | ("CHARACTER SET")) ~> opt("=") ~> ident ^^
            { MysqlCharacterSetTableOption(_) } 
    def tableCollate: Parser[TableOption] =
        "COLLATE" ~> opt("=") ~> ident ^^ { MysqlCollateTableOption(_) }
    
    def tableEngine: Parser[TableOption] =
        ("ENGINE" | "TYPE") ~> opt("=") ~> ident ^^ { MysqlEngineTableOption(_) }
    
    def tableMinMaxRows: Parser[TableOption] =
        ( "MIN_ROWS" ~> opt("=") ~> naturalNumber ^^ { MysqlMinRowsTableOption(_) }
        | "MAX_ROWS" ~> opt("=") ~> naturalNumber ^^ { MysqlMaxRowsTableOption(_) }
        )
    
    def tableComment: Parser[TableOption] =
        ( "COMMENT" ~> opt("=") ~> stringConstant ^^ { MysqlCommentTableOption(_) } )
    
    override def tableOption: Parser[TableOption] =
        ( tableEngine
        | tableDefaultCharset
        | tableCollate
        | tableMinMaxRows
        | tableComment
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
    
    protected def mysqlLike: Parser[TableContentsSource] =
        "LIKE" ~> name ^^ { n => TableElementList(Seq(LikeClause(n))) }
    
    override def tableContentsSource: Parser[TableContentsSource] = super.tableContentsSource | mysqlLike
    
    def setOption: Parser[MysqlSetOptionStatement] =
        ( "SET NAMES" ~> stringValue ^^ { s => MysqlSetOptionStatement("NAMES", s) }
        | "SET" ~> name ~ ("=" ~> stringValue) ^^ { case n ~ v => MysqlSetOptionStatement(n, v) }
        )
    
    override def topLevel = setOption | super.topLevel
}

object MysqlParserCombinator extends MysqlParserCombinator(MysqlContext)

object MysqlParserCombinatorTests extends SqlParserCombinatorTests(MysqlContext) {
    val context = MysqlContext
    
    import context._
    import sqlParserCombinator._
    
    import MysqlTableDdlStatement._
    import TableDdlStatement._
    
    "parse CREATE TABLE ... LIKE" in {
        parse(createTable)("CREATE TABLE oranges LIKE lemons") must beLike {
            case CreateTableStatement("oranges", false,
                    TableElementList(Seq(LikeClause("lemons"))), Seq()) => true
        }
    }
    
    "parser CONSTRAINT ... FOREIGN KEY" in {
        val t = parseCreateTable(
                "CREATE TABLE servers (id INT, dc_id INT, " +
                        "CONSTRAINT dc_fk FOREIGN KEY dc_idx (dc_id) REFERENCES datacenters(id))")
        val fks = t.elements.filter { case _: MysqlForeignKey => true; case _ => false }
        fks must haveSize(1)
        fks.first must beLike {
            case MysqlForeignKey(
                    ForeignKeyModel(Some("dc_fk"), Seq("dc_id"), "datacenters", Seq("id"), _, _),
                    Some("dc_idx"))
                => true
        }
    }
    
    "TABLE options" in {
        val t = parse(createTable)(
            "CREATE TABLE a (id INT) ENGINE=InnoDB MAX_ROWS 10 MIN_ROWS=20 COMMENT 'comm'")
        t.options must contain(MysqlMaxRowsTableOption(10))
        t.options must contain(MysqlMinRowsTableOption(20))
        t.options must contain(MysqlEngineTableOption("InnoDB"))
        t.options must contain(MysqlCommentTableOption("comm"))
    }
    
    "CREATE TABLE named UNIQUE" in {
        val t = parseCreateTable(
            "CREATE TABLE users (login VARCHAR(10), UNIQUE KEY login_key(login))")
        t.uniqueKeys must haveSize(1)
        t.uniqueKeys.first must beLike {
            case UniqueKey(UniqueKeyModel(Some("login_key"), Seq("login"))) => true
        }
    }
    
    "PRIMARY KEY name undocumented grammar" in {
        val t = parseCreateTable(
            "CREATE TABLE df (id INT, PRIMARY KEY pk (id))")
        t.primaryKeys must beLike { case Seq(_) => true }
    }
    
    "quotes in identifiers" in {
        val t = parseCreateTable("""CREATE TABLE `a` (`id` INT, login VARCHAR(100))""")
        t.name must_== "a"
        t.columns must beLike { case Seq(Column("id", _, _), Column("login", _, _)) => true }
    }
    
    "parse ALTER TABLE ADD named UNIQUE INDEX" in {
        val a = parse(alterTable)("ALTER TABLE event ADD UNIQUE INDEX idx2 (cr, d, ex)")
        a must beLike {
            case AlterTableStatement("event",
                    Seq(AddExtra(UniqueKey(UniqueKeyModel(Some("idx2"), Seq("cr", "d", "ex")))))) => true }
    }
    
    "parse ALTER TABLE ADD INDEX" in {
        import AlterTableStatement._
        val a = parse(alterTable)("ALTER TABLE users ADD INDEX (login)")
        a must beLike {
            case AlterTableStatement("users",
                    Seq(AddExtra(Index(IndexModel(None, Seq("login")))))) => true }
    }
    
    "parse indexes" in {
        val t = parseCreateTable("CREATE TABLE a(id INT, UNIQUE(a), INDEX i2(b, c), UNIQUE KEY(d, e))")
        t.indexes must haveSize(1)
        t.indexes(0).index must beLike { case IndexModel(Some("i2"), Seq("b", "c")) => true; case _ => false }
        
        t.uniqueKeys must haveSize(2)
        t.uniqueKeys(0).uk must beLike {
            case UniqueKeyModel(None, Seq("a")) => true }
        t.uniqueKeys(1).uk must beLike {
            case UniqueKeyModel(None, Seq("d", "e")) => true }
    }
    
    "enum" in {
        parse(enum)("ENUM('week', 'month')") must beLike {
            case MysqlEnumDataType(Seq("week", "month")) => true }
        parse(createTable)("CREATE TABLE we (a ENUM('aa', 'bb'))")
    }
    
    "mysqlNumericDataTypeName" in {
        parse(mysqlNumericDataTypeName)("INT") must_== "INT"
    }
    
    "hex number" in {
        parse(numberValue)("0x100") must_== NumberValue(0x100)
        parse(numberValue)("0xabCDE") must_== NumberValue(0xabcde)
    }
}

// vim: set ts=4 sw=4 et:
