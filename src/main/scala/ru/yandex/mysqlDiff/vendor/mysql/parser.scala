package ru.yandex.mysqlDiff.vendor.mysql

import script.parser._

class MysqlParserCombinator(context: Context) extends SqlParserCombinator(context) {
}

object MysqlParserCombinatorTests extends SqlParserCombinatorTests(MysqlContext) {
}

// vim: set ts=4 sw=4 et:
