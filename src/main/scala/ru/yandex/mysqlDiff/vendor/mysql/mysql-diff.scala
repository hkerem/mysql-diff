package ru.yandex.mysqlDiff
package vendor.mysql

import model._
import script._
import diff._

import Implicits._

class MysqlDiffMaker(context: Context) extends DiffMaker(context) {
    override def defaultValuesEquivalent12(a: SqlExpr, b: SqlExpr) = (a, b) match {
        case (MysqlBitSetValue(v), NumberValue(w)) => v == w.longValue
        case _ => super.defaultValuesEquivalent12(a, b)
    }
}

class MysqlDiffSerializer(context: Context) extends DiffSerializer(context) {
    import MysqlTableDdlStatement._
    
    override def dropExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel =>
            AlterTableStatement(table.name, List(
                TableDdlStatement.DropIndex(i.name.getOrThrow("cannot drop unnamed index "+ i +"; table "+ table.name))))
        case _ => super.dropExtraStmt(k, table)
    }
    
    override def createExtraStmt(k: TableExtra, table: TableModel) = k match {
        case i: IndexModel => 
            AlterTableStatement(table.name, List(
                TableDdlStatement.AddIndex(i)))
        case f: ForeignKeyModel =>
            table.findIndexWithColumns(f.localColumnNames) match {
                case Some(IndexModel(name, _, _)) =>
                    AlterTableStatement(table.name, List(
                        TableDdlStatement.AddExtra(MysqlForeignKey(f, name))))
                case None =>
                    AlterTableStatement(table.name, List(
                        TableDdlStatement.AddExtra(MysqlForeignKey(f, None))))
            }
        case _ => super.createExtraStmt(k, table)
    }
}

// vim: set ts=4 sw=4 et:
