package ru.yandex.mysqlDiff

import model._
import script._
import util._

/** Global implicits conversions */
object Implicits {
    // ?
    implicit def toDecls(ps: ColumnProperties) =
        ps.properties.map(CreateTableStatement.ModelColumnProperty(_))
    
    // ?
    implicit def modelCpToCpDecl(cp: ColumnProperty) =
        CreateTableStatement.ModelColumnProperty(cp)
    
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
    
    // JDBC
    
    implicit def jdbcTemplate(ds: LiteDataSource) =
        new JdbcTemplate(ds)
}

// vim: set ts=4 sw=4 et:
