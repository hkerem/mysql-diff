package ru.yandex.mysqlDiff
package vendor.postgresql

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
    
    override def adjustDiffHook(entriesDiff: Seq[TableEntryDiff]) =
        entriesDiff.flatMap {
            case c: ChangeColumnDiff => c.flatten
            case x => List(x)
        }
    
    override def changeColumnStmt(cd: ChangeColumnDiff, table: TableModel) = {
        require(cd.renameTo.isEmpty) // not yet
        require(cd.diff.length == 1)
        AlterTableStatement(table.name, List(AlterColumn(cd.name, columnPropertyDiffStmt(cd.diff.first))))
    }
}

// vim: set ts=4 sw=4 et:
