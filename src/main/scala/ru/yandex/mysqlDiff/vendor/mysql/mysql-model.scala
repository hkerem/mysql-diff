package ru.yandex.mysqlDiff
package vendor.mysql

import model._
import script._

case class MysqlBitSetValue(value: Long) extends SqlValue

object DataTypeValuesKey

// XXX: character set, collation
case class MysqlEnumDataType(values: Seq[String]) extends DataType("ENUM") {
    
    override def equals(that: Any) = that match {
        case that: MysqlEnumDataType => this.values.toList == that.values.toList
        case _ => false
    }
    
    override def customProperties = Seq(DataTypeValuesKey -> values.toList)
}

case class MysqlSetDataType(values: Seq[String]) extends DataType("SET") {
    
    override def equals(that: Any) = that match {
        case that: MysqlSetDataType => this.values.toList == that.values.toList
        case _ => false
    }
    
    override def customProperties = Seq(DataTypeValuesKey -> values.toList)
}

object MysqlDataTypeDecimalsKey
object MysqlDataTypeUnsignedKey
object MysqlDataTypeZerofillKey

case class MysqlNumericDataType(override val name: String, length: Option[Int], decimals: Option[Int],
        unsigned: Option[Boolean], zerofill: Option[Boolean])
    extends DataType(name)
{
    require(MysqlDataTypes.numericDataTypeNames.contains(name), "data type must be a number: " + name)
    require(length.isDefined || decimals.isEmpty)
    
    override def customProperties =
        Seq[(Any, Any)]() ++
        length.map(DataTypeLengthKey -> _) ++
        decimals.map(MysqlDataTypeDecimalsKey -> _) ++
        unsigned.map(MysqlDataTypeUnsignedKey -> _) ++
        zerofill.map(MysqlDataTypeZerofillKey -> _)
        
}

object MysqlCharacterSetKey
object MysqlCollateKey

trait MysqlCharsetAwareDataType extends DataType {
    val charset: Option[String]
    val collate: Option[String]
    def withCharset(cs: Option[String]): this.type
    def withCollate(cl: Option[String]): this.type
    def withDefaultCharset(cs: Option[String]): this.type = {
        if (charset.isDefined) this
        else withCharset(cs)
    }
    def withDefaultCollate(cl: Option[String]): this.type = {
        if (collate.isDefined) this
        else withCollate(cl)
    }
}

case class MysqlCharacterDataType(override val name: String, length: Option[Int],
        charset: Option[String], collate: Option[String])
    extends DataType(name) with MysqlCharsetAwareDataType
{
    require(MysqlDataTypes.characterDataTypeNames.contains(name))
    
    override def customProperties =
        Seq[(Any, Any)]() ++
        length.map(DataTypeLengthKey -> _) ++
        charset.map(MysqlCharacterSetKey -> _) ++
        collate.map(MysqlCollateKey -> _)
    
    override def withCharset(cs: Option[String]): this.type =
        (new MysqlCharacterDataType(name, length, cs, collate)).asInstanceOf[this.type]
    
    override def withCollate(cl: Option[String]): this.type =
        (new MysqlCharacterDataType(name, length, charset, cl)).asInstanceOf[this.type]
}

object MysqlBinaryKey

case class MysqlTextDataType(override val name: String, binary: Option[Boolean],
        charset: Option[String], collate: Option[String])
    extends DataType(name) with MysqlCharsetAwareDataType
{
    require(MysqlDataTypes.textDataTypeNames.contains(name))
    
    override def customProperties =
        Seq[(Any, Any)]() ++
        binary.map(MysqlBinaryKey -> _) ++
        charset.map(MysqlCharacterSetKey -> _) ++
        collate.map(MysqlCollateKey -> _)
    
    override def withCharset(cs: Option[String]): this.type =
        (new MysqlTextDataType(name, binary, cs, collate)).asInstanceOf[this.type]
    
    override def withCollate(cl: Option[String]): this.type =
        (new MysqlTextDataType(name, binary, charset, cl)).asInstanceOf[this.type]
}

object MysqlDataTypes extends DataTypes {
    val numericDataTypeNames = Seq(
        // not sure about bit
        "BIT", "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
        "REAL", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC")
    
    val characterDataTypeNames = Seq("CHAR", "VARCHAR")
    
    val textDataTypeNames = Seq("TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT")
    
    override def make(name0: String, length: Option[Int]) = {
        val name = name0.toUpperCase
        if (numericDataTypeNames.contains(name))
            new MysqlNumericDataType(name, length, None, None, None)
        else if (characterDataTypeNames.contains(name))
            new MysqlCharacterDataType(name, length, None, None)
        else if (textDataTypeNames.contains(name))
            new MysqlTextDataType(name, None, None, None)
        else
            super.make(name0, length)
    }
    
    def int = make("INT")
    
    override def isAnyNumber(name: String) = numericDataTypeNames.contains(name.toUpperCase)
    
    // http://dev.mysql.com/doc/refman/5.0/en/numeric-type-overview.html
    override def normalize(dt: DataType) = super.normalize(dt) match {
        // XXX: only before 5.0.3
        case dt if dt.name == "BIT" => make("TINYINT", Some(1))
        case dt if dt.name == "BOOLEAN" => make("TINYINT", Some(1))
        case dt => dt
    }
    
    def castToBitSet(value: SqlValue) = value match {
        case b: MysqlBitSetValue => b
        case s: StringValue =>
            var r = 0L
            for (c <- s.value) {
                for (i <- 0 until 8) {
                    val bit = (c >> (7 - i)) & 1;
                    r <<= 1
                    r |= bit
                }
            }
            MysqlBitSetValue(r)
        case n: NumberValue => MysqlBitSetValue(n.value.longValue)
        case BooleanValue(true) => MysqlBitSetValue(1)
        case BooleanValue(false) => MysqlBitSetValue(0)
    }
}

object MysqlDataTypesTests extends MySpecification {
    import MysqlContext._
    import modelParser._
    
    "TINYINT(1) equivalent to BIT" in {
        dataTypes.equivalent(dataTypes.make("BIT"), dataTypes.make("TINYINT", Some(1))) must beTrue
    }
    
    "ENUM('abc', 'def') equivalent" in {
        dataTypes.equivalent(
            new MysqlEnumDataType(List("abc", "def")),
            new MysqlEnumDataType(Seq("abc", "def"))) must beTrue
    }
    
    "VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_general_ci equivalent" in {
        val dt = new MysqlCharacterDataType("VARCHAR", Some(100),
                Some("utf8"), Some("utf8_general_ci"))
        dataTypes.equivalent(dt, dt) must beTrue
    }
    
    "castToBitSet" in {
        MysqlDataTypes.castToBitSet(StringValue("\012")) must_== MysqlBitSetValue(012)
    }
}

///
// column properties

case class MysqlOnUpdateCurrentTimestamp(set: Boolean) extends ColumnProperty {
    override def propertyType = MysqlOnUpdateCurrentTimestampPropertyType
}

case object MysqlOnUpdateCurrentTimestampPropertyType extends ColumnPropertyType {
    override type Value = MysqlOnUpdateCurrentTimestamp
}

case class MysqlComment(comment: String) extends ColumnProperty {
    override def propertyType = MysqlCommentPropertyType
}

case object MysqlCommentPropertyType extends ColumnPropertyType {
    override type Value = MysqlComment
}

case class MysqlAutoIncrement(autoIncrement: Boolean) extends ColumnProperty {
    override def propertyType = MysqlAutoIncrementPropertyType
}

case object MysqlAutoIncrementPropertyType extends ColumnPropertyType {
    override type Value = MysqlAutoIncrement
}

///
// table options

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
    
    import ScriptEvaluation.VendorSpecific
    
    case class MysqlVendorSpecific(storageEngine: Option[String]) extends VendorSpecific {
    }
    
    protected override val emptyVendorSpecific = new MysqlVendorSpecific(None)
    
    override def evalStmt(stmt: ScriptStatement, sc: ScriptEvaluation) = stmt match {
        case MysqlSetStatement(NameExpr("storage_engine"), NameExpr(engine)) =>
            sc.mapSpecific(vs => vs.asInstanceOf[MysqlVendorSpecific].copy(storageEngine=Some(engine)))
        // ignore for now
        case MysqlSetStatement(NameExpr(_), _) =>
            sc
        case MysqlSetStatement(VariableExpr(_), _) =>
            sc
        case _ => super.evalStmt(stmt, sc)
    }
    
    // XXX: not sure
    private val foreignKeyIndexExplicit = true
    
    protected override def alterTableOperation(op: Operation, table: TableModel): TableModel = op match {
        case AddExtra(MysqlForeignKey(fk, indexNameOption)) =>
            // MySQL 5.1 seems to ignore foreign keys names
            // http://bitbucket.org/stepancheg/mysql-diff/wiki/EvilMysql/ForeignKeyConstraint
            if (false && indexNameOption.isDefined)
                table
                    .addForeignKey(fk)
                    .addIndex(IndexModel(indexNameOption, fk.localColumns, foreignKeyIndexExplicit))
            else
                table
                    .addForeignKey(fk)
                    .addImplicitIndexes
        case _ => super.alterTableOperation(op, table)
    }
    
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
    
    protected override def parseCreateTableExtra(e: TableElement, ct: CreateTableStatement) = e match {
        case MysqlForeignKey(fk, indexName) =>
            // MySQL 5.1 uses constraint name for index name
            val indexNameToUse = fk.name
            
            // another MySQL magic
            val haveAnotherIndex = ct.elements.exists {
                case Index(IndexModel(_, columns, _)) =>
                    columns.toList.take(fk.localColumns.length) == fk.localColumns.toList
                case UniqueKey(UniqueKeyModel(_, columns)) =>
                    columns.toList.take(fk.localColumns.length) == fk.localColumns.toList
                case PrimaryKey(PrimaryKeyModel(_, columns)) =>
                    columns.toList.take(fk.localColumns.length) == fk.localColumns.toList
                case c @ Column(name, _, props) if props.exists(_ == InlinePrimaryKey) =>
                    List(name).take(fk.localColumns.length) == fk.localColumns.toList
                
                // XXX: what about two foreign keys sharing same index?
                case _ => false
            }
            if (haveAnotherIndex) Seq(fk)
            else Seq(fk, IndexModel(indexNameToUse, fk.localColumns, foreignKeyIndexExplicit))
            /*
            if (true && indexName.isDefined) // XXX: index name is lost
                Seq(fk, IndexModel(indexName, fk.localColumns, foreignKeyIndexExplicit))
            else
                Seq(fk)
            */
        case e => super.parseCreateTableExtra(e, ct)
    }
    
    override def parseCreateTable(ct: script.CreateTableStatement, sc: ScriptEvaluation): ScriptEvaluation = {
        {
            ct.columns.flatMap(_.properties).foreach {
                case f: script.TableDdlStatement.InlineReferences =>
                    // http://dev.mysql.com/doc/refman/5.1/en/create-table.html
                    throw new UnsupportedFeatureException("inline REFERENCES is not supported by MySQL")
                case _ =>
            }
        }
        val scr = super.parseCreateTable(ct, sc)
        
        val scr2 = scr.specific.asInstanceOf[MysqlVendorSpecific].storageEngine match {
            case Some(engine) =>
                scr.alterTable(ct.name, t => t.withDefaultOptions(Seq(MysqlEngineTableOption(engine))))
            case _ => scr
        }
        {
            val t = scr2.db.table(ct.name)
            t.options.find(MysqlEngineTableOptionType) match {
                // XXX: ignore case
                case Some(MysqlEngineTableOption("InnoDB")) =>
                case _ =>
                    if (!t.foreignKeys.isEmpty)
                        throw new UnsupportedFeatureException("FOREIGN KEY is supported only by InnoDB")
            }
        }
        
        val scr3 = scr2.alterTable(ct.name, t => t.withDefaultOptions(Seq(MysqlCommentTableOption(""))))
        
        scr3
    }
    
    protected override def parseColumn(c: Column) = {
        val Column(name, dataType, attrs) = c
        val hasNullability = attrs.exists(_ match {
            case ModelColumnProperty(Nullability(_)) => true
            case _ => false
        })
        val isTimestamp = dataType.name == "TIMESTAMP"
        // TIMESTAMP columns are NOT NULL by default // http://dev.mysql.com/doc/refman/5.1/en/timestamp.html
        if (!hasNullability && isTimestamp)
            super.parseColumn(c.addProperty(ModelColumnProperty(Nullability(false))))
        else
            super.parseColumn(c)
    }
    
    protected override def fixDataType(dataType: DataType, column: ColumnModel, table: TableModel) = {
        // Unspecified collation and charset are taken from table defaults
        // http://dev.mysql.com/doc/refman/5.1/en/charset-column.html
        
        super.fixDataType(dataType, column, table) match {
            case dt: MysqlCharsetAwareDataType =>
                (dt.charset, dt.collate) match {
                    case (Some(cs), Some(cl)) => dt
                    case (Some(cs), None) =>
                        dt.withCollate(MysqlCharsets.defaultCollation(cs))
                    case (None, Some(cl)) =>
                        dt.withCharset(MysqlCharsets.defaultCharset(cl))
                    case (None, None) =>
                        dt.withCharset(tableCharacterSet(table)).withCollate(tableCollation(table))
                }
            case dt => dt
        }
    }
    
    protected override def fixDefaultValue(v: SqlExpr, dt: DataType) = (super.fixDefaultValue(v, dt), dt) match {
        // MySQL boolean is actually int:
        // http://dev.mysql.com/doc/refman/5.0/en/boolean-values.html
        case (BooleanValue(true), _) => NumberValue(1)
        case (BooleanValue(false), _) => NumberValue(0)
        case (v @ StringValue("\0"),
            MysqlNumericDataType("BIT", _, _, _, _) | MysqlNumericDataType("TINYINT", Some(1), _, _, _)) =>
                MysqlDataTypes.castToBitSet(v)
        case (StringValue(s), MysqlNumericDataType(_, _, _, _, _)) =>
            try {
                NumberValue(s)
            } catch {
                case e: Exception =>
                    throw new MysqlDiffException(
                        "cannot parse string value '"+ s +"' as number for type " + dt, e)
            }
        case (NumberValue(0), DefaultDataType("DATE", _)) => StringValue("0000-00-00")
        case (NumberValue(0), DefaultDataType("TIME", _)) => StringValue("00:00:00")
        case (NumberValue(0), DefaultDataType("DATETIME", _)) => StringValue("0000-00-00 00:00:00")
        case (NumberValue(0), DefaultDataType("TIMESTAMP", _)) => StringValue("0000-00-00 00:00:00")
        case (x, _) => x
    }
    
    override def fixColumn(column: ColumnModel, table: TableModel) = {
        if (column.dataType.name == "TIMESTAMP" && column.defaultValue.isEmpty)
            // because of MySQL-specifc features that are hard to deal with
            throw new Exception(
                    "TIMESTAMP without DEFAULT value is prohibited, column " + column.name) // XXX: report table
        super.fixColumn(column, table)
            .withDefaultProperties(Seq(MysqlComment("")))
    }
}

object MysqlModelParserTests extends ModelParserTests(MysqlContext) {
    import MysqlContext._
    import modelParser._
    
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
    
    "unspecified autoincrement" in {
        val t = parseCreateTableScript("CREATE TABLE user (id INT, login VARCHAR(10), PRIMARY KEY(id))")
        t.column("id").properties.find(MysqlAutoIncrementPropertyType) must_== Some(MysqlAutoIncrement(false))
        //t.column("login").properties.autoIncrement must_== None
    }
    
    "unspecified autoincrement" in {
        val t = parseCreateTableScript("CREATE TABLE user (id INT, login VARCHAR(10), PRIMARY KEY(id))")
        t.column("id").properties.find(MysqlAutoIncrementPropertyType) must_== Some(MysqlAutoIncrement(false))
        //t.column("login").properties.autoIncrement must_== None
    }

    "Prohibit TIMESTAMP without DEFAULT value" in {
        val ct = sqlParserCombinator.parseCreateTable(
            "CREATE TABLE x (a TIMESTAMP)")
        try {
            val t = parseCreateTable(ct)
            fail("table should not be allowed, created " + t)
        } catch {
            case e: Exception if e.getMessage contains "prohibited" =>
        }
    }
    
    "COLUMN COMMENT" in {
        val t = modelParser.parseCreateTableScript("CREATE TABLE bottles (size INT COMMENT 'in cm')")
        val c = t.column("size")
        c.properties.find(MysqlCommentPropertyType) must_== Some(MysqlComment("in cm"))
    }
}

class MysqlModelSerializer(context: Context) extends ModelSerializer(context) {
    import context._
    
    import script.TableDdlStatement._
    import MysqlTableDdlStatement._
    
    protected override def splitTable(table: TableModel): (TableModel, Seq[TableExtra]) =
        (table, Nil)
    
    override def serializeColumn(column: ColumnModel) =
        // no need to serialize empty comment
        super.serializeColumn(column.removeProperty(MysqlComment("")))
    
}

// vim: set ts=4 sw=4 et:
