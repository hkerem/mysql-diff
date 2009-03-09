package ru.yandex.mysqlDiff.script

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting._

import model._

// XXX: drop MySQL
import vendor.mysql._

import Implicits._

object ScriptSerializer {
    abstract class Options {
        def stmtJoin: String
        def scriptTail: String = stmtJoin
        def indent = ""
        def afterComma = ""
        def verbose = false
    }
    
    object Options {
        trait Multiline extends Options {
            override def stmtJoin = "\n"
            override def indent = "    "
        }
        
        object multiline extends Multiline
        
        trait Singleline extends Options {
            override def stmtJoin = " "
            override def scriptTail = ""
            override def afterComma = " "
        }
        
        object singleline extends Singleline
    }
    
}

/**
 * Serialize Script objects to text (String).
 */
class ScriptSerializer(context: Context) {
    import context._
    import TableDdlStatement._
    import ScriptSerializer._
    
    /** Serializer options */
    def serialize(stmts: Seq[ScriptElement], options: Options): String = {
        def serializeInList(stmt: ScriptElement) = {
            val tail = stmt match {
                case _: ScriptStatement => ";"
                case _ => ""
            }
            serialize(stmt, options) + tail
        }
        stmts.map(serializeInList _).mkString(options.stmtJoin) + options.scriptTail
    }

    def serialize(stmt: ScriptElement, options: Options): String = stmt match {
        case s: ScriptStatement => serializeStatement(s, options)
        case Unparsed(q) => q
        case CommentElement(c) => c
    }
    
    def serialize(stmt: ScriptElement): String = serialize(stmt, Options.singleline)
    
    def isKeyword(name: String) =
        ScriptConstants.isSql2003Keyword(name)
    
    def quoteName(name: String) = '"' + name + '"'
    
    def serializeName(name: String) =
        if (isKeyword(name)) quoteName(name)
        else name
    
    def serializeTableStatement(stmt: TableDdlStatement, options: Options): String = stmt match {
        case st: CreateTableStatement => serializeCreateTable(st, options)
        case dt: DropTableStatement => serializeDropTable(dt)
        case st: AlterTableStatement => serializeChangeTable(st)
    }
    
    def serializeStatement(stmt: ScriptStatement, options: Options): String = stmt match {
        case ts: TableDdlStatement => serializeTableStatement(ts, options)
        case is: InsertStatement => serializeInsert(is)
    }
    
    def serializeValue(value: SqlValue): String = value match {
        case NullValue => "NULL"
        case NumberValue(number) => number.toString
        case StringValue(string) => "'" + string + "'" // XXX: escape
        case BooleanValue(true) => "TRUE"
        case BooleanValue(false) => "FALSE"
        case NowValue => "NOW()"
        case t: TemporalValue =>
            val n = t match {
                case _: TimestampValue => "TIMESTAMP"
                case _: TimestampWithTimeZoneValue => "TIMESTAMP WITHOUT TIME ZONE"
                case _: TimeValue => "TIME"
                case _: TimeWithTimeZoneValue => "TIME WITHOUT TIME ZONE"
                case _: DateValue => "DATE"
            }
            n + " '" + t.value + "'"
    }
    
    def serializeModelColumnProperty(cp: ColumnProperty): Option[String] = cp match {
        case Nullability(true) => Some("NULL")
        case Nullability(false) => Some("NOT NULL")
        case DefaultValue(value) => Some("DEFAULT " + serializeValue(value))
        case AutoIncrement(true) => Some("AUTO_INCREMENT")
        case AutoIncrement(false) => None
        case OnUpdateCurrentTimestamp(true) => Some("ON UPDATE CURRENT_TIMESTAMP")
        case OnUpdateCurrentTimestamp(false) => None
    }
    
    def serializeColumnProperty(cp: ColumnPropertyDecl): Option[String] = cp match {
        case ModelColumnProperty(cp) => serializeModelColumnProperty(cp)
        case InlineUnique => Some("UNIQUE")
        case InlinePrimaryKey => Some("PRIMARY KEY")
        case InlineReferences(table, columns) => Some("REFERENCES " + table + "(" + columns.mkString(", ") + ")")
    }
    
    def serializeColumnProperty(cp: ColumnPropertyDecl, c: Column): Option[String] =
        cp match {
            // MySQL does not support NOT NULL DEFAULT NULL
            case ModelColumnProperty(DefaultValue(NullValue)) if c.isNotNull => None
            case _ => serializeColumnProperty(cp)
        }
    
    def serializeConstraint(c: Constraint) =
        c match {
            case PrimaryKey(pk) => serializePrimaryKey(pk)
            case ForeignKey(fk) => serializeForeignKey(fk)
            case UniqueKey(u) => serializeUniqueKey(u)
        }
    
    def serializeTableEntry(e: Entry): String = e match {
        case c @ Column(name, dataType, attrs) =>
            name + " " + serializeDataType(dataType) +
                    (if (attrs.isEmpty) ""
                    else " " + attrs.flatMap(cp => serializeColumnProperty(cp, c)).mkString(" "))
        case Index(index) => serializeIndex(index)
        case c: Constraint => serializeConstraint(c)
    }
    
    def serializeCreateTable(t: CreateTableStatement, options: Options): String = {
        def mapEntry(e: Entry) =
            serializeTableEntry(e) + (if (options.verbose) " /* " + e.toString + " */" else "")
        val l = t.entries.map(mapEntry _).reverse
        val lines = (List(l.first) ++ l.drop(1).map(_ + "," + options.afterComma)).reverse.map(options.indent + _)
        
        val firstLine = "CREATE TABLE " + serializeName(t.name) + " ("
        val lastLine = ")" +
            (if (t.options.isEmpty) "" else " " + t.options.map(serializeTableOption _).mkString(" "))
        
        (List(firstLine) ++ lines ++ List(lastLine)).mkString(options.stmtJoin)
    }
    
    def serializeTableOption(opt: TableOption) = opt match {
        case MysqlCollateTableOption(name) => "COLLATE=" + name
        case MysqlCharacterSetTableOption(name) => "CHARACTER SET=" + name
        case MysqlEngineTableOption(name) => "ENGINE=" + name
    }
    
    def serializeInsert(is: InsertStatement) = {
        val r = new ArrayBuffer[String]
        r += "INSERT"
        if (is.ignore) r += "IGNORE"
        r += "INTO"
        r += is.table
        if (is.columns.isDefined)
            r += ("(" + is.columns.get.mkString(", ") + ")")
        r += "VALUES"
        r += is.data.map(row => "(" + row.map(serializeValue _).mkString(", ") + ")").mkString(", ")
        r.mkString(" ")
        // XXX: untested
    }
    
    def serializeDropTable(dt: DropTableStatement) = {
        val DropTableStatement(name, ifExists) = dt
        val words = new ArrayBuffer[String]
        words += "DROP TABLE"
        if (ifExists) words += "IF EXISTS"
        words += name
        words.mkString(" ")
    }
    
    def serializeChangeTable(st: AlterTableStatement) =
        "ALTER TABLE " + st.name + " " +
            st.ops.map(serializeAlterTableOperation(_)).mkString(", ")
    
    def serializeAlterTableOperation(op: Operation) = {
        op match {
            case AddEntry(column: Column) => "ADD COLUMN " + serializeTableEntry(column)
            case AddEntry(e) => "ADD " + serializeTableEntry(e)
            
            case ChangeColumn(oldName, column) => "CHANGE COLUMN " + oldName + " " + serializeColumn(column)
            case ModifyColumn(column) => "MODIFY COLUMN " + serializeColumn(column)
            case DropColumn(name) => "DROP COLUMN " + name
            
            case DropPrimaryKey => "DROP PRIMARY KEY"
            case DropIndex(name) => "DROP INDEX " + name
            case DropForeignKey(name) => "DROP FOREIGN KEY " + name
            
            case ChangeTableOption(o) => serializeTableOption(o)
        }
    }
    
    def serializeDataTypeOption(o: DataTypeOption) = o match {
        case MysqlUnsigned(true) => Some("UNSIGNED")
        case MysqlUnsigned(false) => None
        case MysqlZerofill(true) => Some("ZEROFILL")
        case MysqlZerofill(false) => None
        case MysqlCharacterSet(cs) => Some("CHARACTER SET " + cs)
        case MysqlCollate(collate) => Some("COLLATE " + collate)
    }
   
    def serializeDataType(dataType: DataType) = {
        val words = new ArrayBuffer[String]
        words += {
            var size = dataType.length.map("(" + _ + ")").getOrElse("")
            dataType.name + size
        }
        
        // Hack: MySQL requires CHARACTER SET before COLLATE
        def dataTypeOptionOrder(o: DataTypeOption) = o match {
            case x: MysqlCollate => 2
            case x: MysqlCharacterSet => 1
            case x => 0
        }
        words ++= stableSort(dataType.options.properties, dataTypeOptionOrder _)
                .flatMap(serializeDataTypeOption _)
        
        words.mkString(" ")
    }
    
    def serializeColumn(model: ColumnModel) =
        serializeTableEntry(modelSerializer.serializeColumn(model))
    
    def serializePrimaryKey(pk: PrimaryKeyModel) = {
        val words = new ArrayBuffer[String]
        if (pk.name.isDefined) words += "CONSTRAINT " + pk.name.get
        words += "PRIMARY KEY"
        words += ("(" + pk.columns.mkString(", ") + ")")
        words.mkString(" ")
    }
    
    // XXX: handle MySQL specific stuff
    def serializeForeignKey(fk: ForeignKeyModel) = {
        val words = new ArrayBuffer[String]
        if (fk.name.isDefined) words += "CONSTRAINT " + fk.name.get
        words += "FOREIGN KEY"
        words += ("(" + fk.localColumns.mkString(", ") + ")")
        words += "REFERENCES"
        words += fk.externalTable
        words += ("(" + fk.externalColumns.mkString(", ") + ")")
        words.mkString(" ")
    }
    
    def serializeUniqueKey(uk: UniqueKeyModel) = {
        val words = new ArrayBuffer[String]
        if (uk.name.isDefined) words += "CONSTRAINT " + uk.name.get
        words += "UNIQUE KEY(" + uk.columns.mkString(", ") + ")"
        words.mkString(" ")
    }
    
    def serializeIndex(index: IndexModel) = {
        val words = new ArrayBuffer[String]
        words += "INDEX"
        words ++= index.name
        words += ("(" + index.columns.mkString(", ") + ")")
        words.mkString(" ")
    }
    
}

object ScriptSerializerTests extends org.specs.Specification {
    val context = Environment.defaultContext
    import context._
    
    import ScriptSerializer._
    
    "serialize semi singleline" in {
        val dt = DropTableStatement("users", false)
        val c = CommentElement("/* h */")
        
        val script = List(dt, c, dt, dt, c, c, dt)
        
        val options = Options.singleline
        
        val serialized = scriptSerializer.serialize(script, options)
        
        //println("'" + serialized + "'")
        assert(serialized == "DROP TABLE users; /* h */ DROP TABLE users; DROP TABLE users; /* h */ /* h */ DROP TABLE users;")
    }
    
    "serialize default value" in {
        scriptSerializer.serializeModelColumnProperty(DefaultValue(NumberValue(15))).get must_== "DEFAULT 15"
    }
    
    "serialize value" in {
        scriptSerializer.serializeValue(NullValue) must_== "NULL"
        scriptSerializer.serializeValue(NumberValue(15)) must_== "15"
        scriptSerializer.serializeValue(StringValue("hello")) must_== "'hello'"
        scriptSerializer.serializeValue(NowValue) must_== "NOW()" // XXX: or CURRENT_TIMESTAMP
        scriptSerializer.serializeValue(DateValue("2009-03-09")) must_== "DATE '2009-03-09'"
    }
}

// vim: set ts=4 sw=4 et:
