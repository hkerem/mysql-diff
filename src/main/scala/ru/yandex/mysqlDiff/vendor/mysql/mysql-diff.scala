package ru.yandex.mysqlDiff.vendor.mysql

import model._
import script._
import diff._

import Implicits._

class MysqlDiffSerializer(context: Context) extends DiffSerializer(context) {
    override def dropExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel =>
            AlterTableStatement(table.name, List(
                TableDdlStatement.DropIndex(i.name.getOrThrow("cannot drop unnamed index"))))
        case _ => super.dropExtraStmt(k, table)
    }
    
    override def createExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel => 
            AlterTableStatement(table.name, List(
                TableDdlStatement.AddIndex(i)))
        case _ => super.createExtraStmt(k, table)
    }
}

// vim: set ts=4 sw=4 et:
