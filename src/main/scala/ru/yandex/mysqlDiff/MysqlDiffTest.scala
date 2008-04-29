package ru.yandex.mysqlDiff

import scalax.testing._

object MysqlDiffTest extends TestSuite("MySQL diff full test") {
    include(ru.yandex.mysqlDiff.diff.DiffTest);
    include(ru.yandex.mysqlDiff.diff.ScriptBulderTest);
    include(ru.yandex.mysqlDiff.diff.TextParserTest);
}
