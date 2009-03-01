package ru.yandex.mysqlDiff.vendor.mysql

import model._

case class MysqlZerofill(set: Boolean) extends DataTypeOption {
    override def propertyType = MysqlZerofillType
}
case object MysqlZerofillType extends DataTypeOptionType {
    override type Value = MysqlZerofill
}

case class MysqlUnsigned(set: Boolean) extends DataTypeOption {
    override def propertyType = MysqlUnsignedType
}
case object MysqlUnsignedType extends DataTypeOptionType {
    override type Value = MysqlUnsigned
}

case class MysqlCharacterSet(name: String) extends DataTypeOption {
    override def propertyType = MysqlCharacterSetType
}
case object MysqlCharacterSetType extends DataTypeOptionType {
    override type Value = MysqlCharacterSet
}

case class MysqlCollate(name: String) extends DataTypeOption {
    override def propertyType = MysqlCollateType
}
case object MysqlCollateType extends DataTypeOptionType {
    override type Value = MysqlCollate
}


object MysqlDataTypes extends DataTypes {
    def int = make("INT")

    override def normalize(dt: DataType) = super.normalize(dt) match {
        case DataType("BIT", _, options) => make("TINYINT", Some(1), options)
        case dt => dt
    }
    
}

object MysqlDataTypesTests extends org.specs.Specification {
    import MysqlContext._

    "TINYINT(1) equivalent to BIT" in {
        dataTypes.equivalent(dataTypes.make("BIT"), dataTypes.make("TINYINT", Some(1))) must beTrue
    }
}

case class MysqlEngineTableOption(engine: String) extends TableOption {
    override def propertyType = MysqlEngineTableOptionType
}

object MysqlEngineTableOptionType extends TableOptionType {
    override type Value = MysqlEngineTableOption
}

case class MysqlCharacterSetTableOption(name: String) extends TableOption {
    override def propertyType = MysqlCharacterSetTableOptionType
}

object MysqlCharacterSetTableOptionType extends TableOptionType {
    override type Value = MysqlCharacterSetTableOption
}

case class MysqlCollateTableOption(name: String) extends TableOption {
    override def propertyType = MysqlCollateTableOptionType
}

object MysqlCollateTableOptionType extends TableOptionType {
    override type Value = MysqlCollateTableOption
}

class MysqlModelParser(override val context: Context) extends ModelParser(context) {
    private def tableCollation(table: TableModel): Option[String] = {
        def defaultCollation =
            table.options.find(MysqlCharacterSetTableOptionType).flatMap(
                o => MysqlCharsets.defaultCollation(o.name))
        table.options.find(MysqlCollateTableOptionType).map(_.name).orElse(defaultCollation)
    }
    
    private def tableCharacterSet(table: TableModel): Option[String] = {
        def defaultCharset = 
            table.options.find(MysqlCollateTableOptionType).flatMap(
                o => MysqlCharsets.defaultCharset(o.name))
        table.options.find(MysqlCharacterSetTableOptionType).map(_.name).orElse(defaultCharset)
    }
    
    protected override def fixDataType(dataType: DataType, column: ColumnModel, table: TableModel) = {
        // http://dev.mysql.com/doc/refman/5.1/en/charset-column.html
        
        val defaultCharset: Option[MysqlCharacterSet] =
            dataType.options.find(MysqlCollateType).flatMap {
                    cl: MysqlCollate => MysqlCharsets.defaultCharset(cl.name) }
                .orElse(tableCharacterSet(table))
                .map(MysqlCharacterSet(_))
        val defaultCollation: Option[MysqlCollate] =
            dataType.options.find(MysqlCharacterSetType).flatMap {
                    cs: MysqlCharacterSet => MysqlCharsets.defaultCollation(cs.name) }
                .orElse(tableCollation(table))
                .map(MysqlCollate(_))
        
        val defaultOptions: Seq[DataTypeOption] = List[DataTypeOption]() ++ defaultCharset ++ defaultCollation
        
        dataType.withOptions(dataType.options.withDefaultProperties(defaultOptions))
    }
}

// vim: set ts=4 sw=4 et:
