package ru.yandex.mysqlDiff.vendor.mysql

import script.parser._

class MysqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
    override def dataTypeOption = (
        super.dataTypeOption
      | ("UNSIGNED" ^^^ MysqlUnsigned)
      | ("ZEROFILL" ^^^ MysqlZerofill)
      | ("CHARACTER" ~> "SET" ~> name ^^ (name => MysqlCharacterSet(name)))
      | ("COLLATE" ~> name ^^ (name => MysqlCollate(name)))
    )
}

object MysqlParserCombinatorTests extends SqlParserCombinatorTests(MysqlContext) {
    val context = MysqlContext
    
    import context._
    import sqlParserCombinator._
    
    "parse dataTypeOption" in {
        parse(dataTypeOption)("UNSIGNED") must_== MysqlUnsigned
        parse(dataTypeOption)("CHARACTER SET utf8") must_== new MysqlCharacterSet("utf8")
        parse(dataTypeOption)("COLLATE utf8bin") must_== new MysqlCollate("utf8bin")
    }
}

// vim: set ts=4 sw=4 et:
