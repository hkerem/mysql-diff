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


// XXX: character set, collation
case class MysqlEnumDataType(values: Seq[String]) extends DataType {
    override val name = "ENUM"
}
case class MysqlSetDataType(values: Seq[String]) extends DataType {
    override val name = "SET"
}


object MysqlDataTypes extends DataTypes {
    def int = make("INT")

    // http://dev.mysql.com/doc/refman/5.0/en/numeric-type-overview.html
    override def normalize(dt: DataType) = super.normalize(dt) match {
        // XXX: only before 5.0.3
        case DefaultDataType("BIT", _, options) => new DefaultDataType("TINYINT", Some(1), options)
        case DefaultDataType("BOOLEAN", _, options) => new DefaultDataType("TINYINT", Some(1), options)
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

case class MysqlAutoIncrementTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlAutoIncrementTableOptionType
}

case object MysqlAutoIncrementTableOptionType extends TableOptionType {
    override type Value = MysqlAutoIncrementTableOption
}

case class MysqlAvgRowLengthTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlAvgRowLengthTableOptionType
}

case object MysqlAvgRowLengthTableOptionType extends TableOptionType {
    override type Value = MysqlAvgRowLengthTableOption
}

case class MysqlChecksumTableOption(value: Boolean) extends TableOption {
    override def propertyType = MysqlChecksumTableOptionType
}

case object MysqlChecksumTableOptionType extends TableOptionType {
    override type Value = MysqlChecksumTableOption
}

case class MysqlCommentTableOption(value: String) extends TableOption {
    override def propertyType = MysqlCommentTableOptionType
}

case object MysqlCommentTableOptionType extends TableOptionType {
    override type Value = MysqlCommentTableOption
}

case class MysqlConnectionTableOption(value: String) extends TableOption {
    override def propertyType = MysqlConnectionTableOptionType
}

case object MysqlConnectionTableOptionType extends TableOptionType {
    override type Value = MysqlConnectionTableOption
}

case class MysqlDataDirectoryTableOption(value: String) extends TableOption {
    override def propertyType = MysqlDataDirectoryTableOptionType
}

case object MysqlDataDirectoryTableOptionType extends TableOptionType {
    override type Value = MysqlDataDirectoryTableOption
}

case class MysqlDelayKeyWriteTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlDelayKeyWriteTableOptionType
}

case object MysqlDelayKeyWriteTableOptionType extends TableOptionType {
    override type Value = MysqlDelayKeyWriteTableOption
}

case class MysqlIndexDirectoryTableOption(value: String) extends TableOption {
    override def propertyType = MysqlIndexDirectoryTableOptionType
}

case object MysqlIndexDirectoryTableOptionType extends TableOptionType {
    override type Value = MysqlIndexDirectoryTableOption
}

// XXX: should be enum
case class MysqlInsertMethodTableOption(value: String) extends TableOption {
    override def propertyType = MysqlInsertMethodTableOptionType
}

case object MysqlInsertMethodTableOptionType extends TableOptionType {
    override type Value = MysqlInsertMethodTableOption
}

case class MysqlKeyBlockSizeTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlKeyBlockSizeTableOptionType
}

case object MysqlKeyBlockSizeTableOptionType extends TableOptionType {
    override type Value = MysqlKeyBlockSizeTableOption
}

case class MysqlMaxRowsTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlMaxRowsTableOptionType
}

case object MysqlMaxRowsTableOptionType extends TableOptionType {
    override type Value = MysqlMaxRowsTableOption
}

case class MysqlMinRowsTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlMinRowsTableOptionType
}

case object MysqlMinRowsTableOptionType extends TableOptionType {
    override type Value = MysqlMinRowsTableOption
}

case class MysqlPackKeysTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlPackKeysTableOptionType
}

case object MysqlPackKeysTableOptionType extends TableOptionType {
    override type Value = MysqlPackKeysTableOption
}

case class MysqlPasswordTableOption(value: String) extends TableOption {
    override def propertyType = MysqlPasswordTableOptionType
}

case object MysqlPasswordTableOptionType extends TableOptionType {
    override type Value = MysqlPasswordTableOption
}

case class MysqlRowFormatTableOption(value: String) extends TableOption {
    override def propertyType = MysqlRowFormatTableOptionType
}

case object MysqlRowFormatTableOptionType extends TableOptionType {
    override type Value = MysqlRowFormatTableOption
}

// XXX: write something meaningful
case class MysqlTablespaceTableOption(value: Int) extends TableOption {
    override def propertyType = MysqlTablespaceTableOptionType
}

case object MysqlTablespaceTableOptionType extends TableOptionType {
    override type Value = MysqlTablespaceTableOption
}

case class MysqlUnionTableOption(value: Seq[String]) extends TableOption {
    override def propertyType = MysqlUnionTableOptionType
}

case object MysqlUnionTableOptionType extends TableOptionType {
    override type Value = MysqlUnionTableOption
}


class MysqlModelParser(override val context: Context) extends ModelParser(context) {
    import context._
    
    import script.TableDdlStatement._
    import MysqlTableDdlStatement._
    
    /** Unspecified collation can be computed from charset */
    private def tableCollation(table: TableModel): Option[String] = {
        def defaultCollation =
            table.options.find(MysqlCharacterSetTableOptionType).flatMap(
                o => MysqlCharsets.defaultCollation(o.name))
        table.options.find(MysqlCollateTableOptionType).map(_.name).orElse(defaultCollation)
    }
    
    /** Unspecified charset can be computed from collation */
    private def tableCharacterSet(table: TableModel): Option[String] = {
        def defaultCharset = 
            table.options.find(MysqlCollateTableOptionType).flatMap(
                o => MysqlCharsets.defaultCharset(o.name))
        table.options.find(MysqlCharacterSetTableOptionType).map(_.name).orElse(defaultCharset)
    }
    
    protected override def parseCreateTableExtra(e: Entry) = e match {
        case MysqlForeignKey(fk, indexName) => Seq(fk, IndexModel(indexName, fk.localColumns))
        case e => super.parseCreateTableExtra(e)
    }
    
    override def parseCreateTable(ct: script.CreateTableStatement): TableModel = {
        ct.columns.flatMap(_.properties).foreach {
            case f: script.TableDdlStatement.InlineReferences =>
                // http://dev.mysql.com/doc/refman/5.1/en/create-table.html
                throw new UnsupportedFeatureException("inline REFERENCES is not supported by MySQL")
            case _ =>
        }
        val t = super.parseCreateTable(ct)
        t.options.find(MysqlEngineTableOptionType) match {
            // XXX: ignore case
            case Some(MysqlEngineTableOption("InnoDB")) =>
            case _ =>
                if (!t.foreignKeys.isEmpty)
                    throw new UnsupportedFeatureException("FOREIGN KEY is supported only by InnoDB")
        }
        t
    }
    
    protected override def fixDataType(dataType: DataType, column: ColumnModel, table: TableModel) = {
        // Unspecified collation and charset are taken from table defaults
        // http://dev.mysql.com/doc/refman/5.1/en/charset-column.html
        
        val defaultCharset: Option[MysqlCharacterSet] = dataType match {
            case DefaultDataType(name, _, options) if dataTypes.isAnyChar(name) =>
                options.find(MysqlCollateType).flatMap {
                        cl: MysqlCollate => MysqlCharsets.defaultCharset(cl.name) }
                    .orElse(tableCharacterSet(table))
                    .map(MysqlCharacterSet(_))
            case _ => None
        }
        val defaultCollation: Option[MysqlCollate] = dataType match {
            case DefaultDataType(name, _, options) if dataTypes.isAnyChar(name) =>
                options.find(MysqlCharacterSetType).flatMap {
                        cs: MysqlCharacterSet => MysqlCharsets.defaultCollation(cs.name) }
                    .orElse(tableCollation(table))
                    .map(MysqlCollate(_))
            case _ => None
        }
        
        val defaultOptions: Seq[DataTypeOption] = List[DataTypeOption]() ++ defaultCharset ++ defaultCollation
        
        dataType match {
            case d: DefaultDataType => d.withDefaultOptions(defaultOptions)
            case d if defaultOptions.isEmpty => d
        }
    }
    
    private def fixDefaultValue(v: SqlValue) = v match {
        // MySQL boolean is actually int:
        // http://dev.mysql.com/doc/refman/5.0/en/boolean-values.html
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

object MysqlModelParserTests extends ModelParserTests(MysqlContext) {
    import MysqlContext._
    
    "INT column must have no collation" in {
        val t = modelParser.parseCreateTableScript(
            "CREATE TABLE users (id INT) CHARACTER SET utf8 COLLATE utf8_bin")
        val c = t.column("id")
        c.dataType.asInstanceOf[DefaultDataType].options.find(MysqlCollateType) must_== None
        c.dataType.asInstanceOf[DefaultDataType].options.find(MysqlCharacterSetType) must_== None
    }
    
    "parse CREATE TABLE LIKE" in {
        val s = "CREATE TABLE a (id INT); CREATE TABLE b LIKE a"
        val db = modelParser.parseModel(s)
        db.table("a") must beLike { case TableModel("a", _, _, _) => true }
        db.table("b") must beLike { case TableModel("b", _, _, _) => true }
        db.table("b").columns must beLike { case Seq(ColumnModel("id", _, _)) => true }
    }
    
    // http://dev.mysql.com/doc/refman/5.1/en/create-table.html
    "disable inline REFERENCES (stupid MySQL)" in {
        try {
            modelParser.parseModel("CREATE TABLE a (b_id INT REFERENCES b(id))")
            fail("inline REFERENCES is not supported by MySQL")
        } catch {
            case _: UnsupportedFeatureException =>
        }
    }
    
    // http://dev.mysql.com/doc/refman/5.1/en/create-table.html
    "FOREIGN KEY is not supported by MySQL in MyISAM" in {
        try {
            modelParser.parseModel("CREATE TABLE a (b_id INT, FOREIGN KEY (id) REFERENCES b(b_id)) ENGINE=MyISAM")
            fail("FOREIGN KEY is not supported by MyISAM")
        } catch {
            case _: UnsupportedFeatureException =>
        }
    }
    
    "FOREIGN KEY" in {
        val t = modelParser.parseCreateTableScript(
            "CREATE TABLE a (b_id INT, FOREIGN KEY (id) REFERENCES b(b_id)) ENGINE=InnoDB")
        t.foreignKeys must haveSize(1)
    }
    
}

// vim: set ts=4 sw=4 et:
