package ru.yandex.mysqlDiff;

import scalax.testing._

object MySqlDiffTest extends TestSuite("MySQL diff full test") {
  include(ru.yandex.mysqlDiff.diff.simple.SimpleDiffTest);
  include(ru.yandex.mysqlDiff.diff.simple.ScriptBulderTest);
  include(ru.yandex.mysqlDiff.diff.simple.SimpleTextHarvesterTest);
}