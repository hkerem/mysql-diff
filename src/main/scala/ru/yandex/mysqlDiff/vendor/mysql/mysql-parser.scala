package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._

class MysqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
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
   
    override def tableOption: Parser[TableOption] = (
        tableEngine
      | tableDefaultCharset
      | tableCollate
    )
    
    def convertToCharacterSet =
        "CONVERT TO CHARACTER SET" ~> name ~ opt("COLLATE" ~> name) ^^
            { case cs ~ coll => MysqlAlterTableStatement.ConvertToCharacterSet(cs, coll) }
    
    def changeCharacterSet =
        opt("DEFAULT") ~> "CHARACTER SET" ~> opt("=") ~> name ~ opt("COLLATE" ~> opt("=") ~> name) ^^
            { case cs ~ coll => MysqlAlterTableStatement.ChangeCharacterSet(cs, coll) }
    
    // http://dev.mysql.com/doc/refman/5.1/en/alter-table.html
    override def alterSpecification = super.alterSpecification | convertToCharacterSet | changeCharacterSet
    
    def createTableLike: Parser[CreateTableLikeStatement] =
            "CREATE TABLE" ~> opt("IF NOT EXISTS") ~ name ~ ("LIKE" ~> name) ^^ {
                case ifne ~ name ~ likeName => CreateTableLikeStatement(name, ifne.isDefined, likeName) }
    
    override def createTable = super.createTable | createTableLike
}

object MysqlParserCombinatorTests extends SqlParserCombinatorTests(MysqlContext) {
    val context = MysqlContext
    
    import context._
    import sqlParserCombinator._
    
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
}

// vim: set ts=4 sw=4 et:
