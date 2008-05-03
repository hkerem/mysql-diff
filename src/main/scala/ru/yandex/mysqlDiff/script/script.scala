package ru.yandex.mysqlDiff.script

import model._

class Script(val stmts: Seq[ScriptStatement])

abstract class ScriptStatement

case class CreateTableStatement(model: TableModel) extends ScriptStatement

// vim: set ts=4 sw=4 et:
