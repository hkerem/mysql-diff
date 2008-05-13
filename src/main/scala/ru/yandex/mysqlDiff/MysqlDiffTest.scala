package ru.yandex.mysqlDiff

import scalax.testing._

object MysqlDiffTest extends TestSuite("MySQL diff full test") {
    include(diff.DiffTest)
    include(diff.DiffSerializerTest)
    include(diff.DiffSerializerTest)
}

// vim: set ts=4 sw=4 et:
