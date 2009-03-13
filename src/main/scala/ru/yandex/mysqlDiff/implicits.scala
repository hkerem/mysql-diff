package ru.yandex.mysqlDiff

import model._
import script._
import util._

/** Global implicits conversions */
object Implicits extends util.CollectionImplicits with util.JdbcImplicits with util.StringImplicits {
    // I believe it must be added to scala.Predef
    type ArrayBuffer[T] = scala.collection.mutable.ArrayBuffer[T]
    
    // ?
    implicit def toDecls(ps: ColumnProperties) =
        ps.properties.map(TableDdlStatement.ModelColumnProperty(_))
    
    // ?
    implicit def modelCpToCpDecl(cp: ColumnProperty) =
        TableDdlStatement.ModelColumnProperty(cp)
    
    /// script
    
    implicit def toScript(stmts: Seq[ScriptElement]) =
        new Script(stmts)
    
    ///
    // PropertyMap converters
    
    implicit def toColumnProperties(ps: Seq[ColumnProperty]) =
        new ColumnProperties(ps)
    
    implicit def fromColumnProperties(ps: ColumnProperties) =
        ps.properties
    
    implicit def toDataTypeOptions(os: Seq[DataTypeOption]) =
        new DataTypeOptions(os)
    
    implicit def fromDataTypeOptions(os: DataTypeOptions) =
        os.properties
    
    implicit def toTableOptions(to: Seq[TableOption]) =
        new TableOptions(to)
    
    implicit def fromTableOptions(to: TableOptions) =
        to.properties
    
}

// vim: set ts=4 sw=4 et:
