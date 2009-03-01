package ru.yandex.mysqlDiff

import model._
import script._

/** Global implicits conversions */
object Implicits {
    // ?
    implicit def toDecls(ps: ColumnProperties) =
        ps.properties.map(CreateTableStatement.ModelColumnProperty(_))
    
    // ?
    implicit def modelCpToCpDecl(cp: ColumnProperty) =
        CreateTableStatement.ModelColumnProperty(cp)
    
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
}

// vim: set ts=4 sw=4 et:
