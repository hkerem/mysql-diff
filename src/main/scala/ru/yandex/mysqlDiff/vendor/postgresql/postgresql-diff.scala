package ru.yandex.mysqlDiff.vendor.postgresql

import diff._
import script._
import model._

class PostgresqlDiffSerializer(override val context: Context) extends DiffSerializer(context) {
    import context._
    import TableDdlStatement._
    import PostgresqlTableDdlStatement._
    
    // XXX: move to common
    private def changeColumnPropertyDiffStmt(pd: ChangeColumnPropertyDiff): AlterColumnOperation =
        pd.newProperty match {
            case Nullability(n) => SetNotNull(!n)
            case DataTypeProperty(dt) => ChangeType(dt)
            
            // // SET DEFAULT NULL causes default to be 'NULL::character varying' somewhy
            case DefaultValue(NullValue) => SetDefault(None)
            
            case DefaultValue(value) => SetDefault(Some(value))
        }
    
    private def columnPropertyDiffStmt(pd: ColumnPropertyDiff): AlterColumnOperation =
        pd match {
            case c: ChangeColumnPropertyDiff => changeColumnPropertyDiffStmt(c)
        }
    
    override def changeColumnStmts(cd: ChangeColumnDiff, table: TableModel) = {
        val ChangeColumnDiff(name, newName, diff) = cd
        val stmts = diff.map(pd => AlterColumn(name, columnPropertyDiffStmt(pd)))
        if (newName.isDefined)
            stmts ++ List(RenameColumn(name, newName.get))
        else
            stmts
    }
}

// vim: set ts=4 sw=4 et:
