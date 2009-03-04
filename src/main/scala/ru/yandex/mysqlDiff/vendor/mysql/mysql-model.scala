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

    // http://dev.mysql.com/doc/refman/5.0/en/numeric-type-overview.html
    override def normalize(dt: DataType) = super.normalize(dt) match {
        // XXX: only before 5.0.3
        case DataType("BIT", _, options) => make("TINYINT", Some(1), options)
        case DataType("BOOLEAN", _, options) => make("TINYINT", Some(1), options)
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
    import context._
    
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
            if (dataTypes.isAnyChar(dataType.name))
                dataType.options.find(MysqlCollateType).flatMap {
                        cl: MysqlCollate => MysqlCharsets.defaultCharset(cl.name) }
                    .orElse(tableCharacterSet(table))
                    .map(MysqlCharacterSet(_))
            else None
        val defaultCollation: Option[MysqlCollate] =
            if (dataTypes.isAnyChar(dataType.name))
                dataType.options.find(MysqlCharacterSetType).flatMap {
                        cs: MysqlCharacterSet => MysqlCharsets.defaultCollation(cs.name) }
                    .orElse(tableCollation(table))
                    .map(MysqlCollate(_))
            else None
        
        val defaultOptions: Seq[DataTypeOption] = List[DataTypeOption]() ++ defaultCharset ++ defaultCollation
        
        dataType.withOptions(dataType.options.withDefaultProperties(defaultOptions))
    }
    
    private def fixDefaultValue(v: SqlValue) = v match {
        case BooleanValue(true) => NumberValue(1)
        case BooleanValue(false) => NumberValue(0)
        case x => x
    }
    
    private def fixDefaultValueProperty(v: DefaultValue) =
        DefaultValue(fixDefaultValue(v.value))
    
    protected override def fixColumn(column: ColumnModel, table: TableModel) = {
        val superFixed = super.fixColumn(column, table)
        superFixed.overrideProperties(
                superFixed.properties.find(DefaultValuePropertyType).map(fixDefaultValueProperty _).toList)
    }
}

object MysqlModelParserTests extends org.specs.Specification {
    import MysqlContext._
    
    "INT column must have no collation" in {
        val t = modelParser.parseCreateTableScript(
            "CREATE TABLE users (id INT) CHARACTER SET utf8 COLLATE utf8_bin")
        val c = t.column("id")
        c.dataType.options.find(MysqlCollateType) must_== None
        c.dataType.options.find(MysqlCharacterSetType) must_== None
    }
}

// vim: set ts=4 sw=4 et:
