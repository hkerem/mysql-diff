package ru.yandex.mysqlDiff
package script

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting._

import model._

// XXX: drop MySQL
import vendor.mysql._

import Implicits._

object ScriptSerializer {
    case class Options(multiline: Boolean = false) {
        def stmtJoin =
            if (multiline) "\n"
            else " "
    }
    
    object Options {
        val singleline = new Options()
        val multiline = singleline.copy(true)
        
        val default = singleline
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
        val stmtTail =
            if (options.multiline)
                "\n"
            else
                ""
        
        stmts.map(serializeInList _).mkString(options.stmtJoin) + stmtTail
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
    
    def serializeString(string: String) =
        "'" + string + "'" // XXX: escape
    
    def serializeTableDdlStatement(stmt: TableDdlStatement, options: Options): String = stmt match {
        case st: CreateTableStatement => serializeCreateTable(st, options)
        case dt: DropTableStatement => serializeDropTable(dt)
        case st: AlterTableStatement => serializeChangeTable(st)
    }
    
    def serializeSequenceDdlStatement(stmt: SequenceDdlStatement, options: Options): String = stmt match {
        case CreateSequenceStatement(name: String) => "CREATE SEQUENCE " + serializeName(name)
        case DropSequenceStatement(name: String) => "DROP SEQUENCE " + serializeName(name)
    }
    
    def serializeCreateIndexStatement(stmt: CreateIndexStatement) = {
        val CreateIndexStatement(name, tableName, columns) = stmt
        "CREATE INDEX " + serializeName(name) +
            " ON " + serializeName(tableName) + " (" + columns.map(serializeIndexColumn _).mkString(", ") + ")"
    }
    
    def serializeDropIndexStatement(stmt: DropIndexStatement) =
        "DROP INDEX " + serializeName(stmt.name)
    
    def serializeIndexDdlStatement(stmt: IndexDdlStatement, options: Options): String = stmt match {
        case stmt: CreateIndexStatement => serializeCreateIndexStatement(stmt)
        case stmt: DropIndexStatement => serializeDropIndexStatement(stmt)
    }
    
    def serializeDdlStatement(stmt: DdlStatement, options: Options): String = stmt match {
        case stmt: TableDdlStatement => serializeTableDdlStatement(stmt, options)
        case stmt: SequenceDdlStatement => serializeSequenceDdlStatement(stmt, options)
        case stmt: IndexDdlStatement => serializeIndexDdlStatement(stmt, options)
    }
    
    def serializeStatement(stmt: ScriptStatement, options: Options): String = stmt match {
        case ts: DdlStatement => serializeDdlStatement(ts, options)
        case is: InsertStatement => serializeInsert(is)
    }
    
    def serializeValue(value: SqlValue): String = value match {
        case NullValue => "NULL"
        case NumberValue(number) => number.toString
        case StringValue(string) => serializeString(string)
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
    
    def serializeCast(cast: CastExpr) = {
        val CastExpr(e, as) = cast
        "CAST(" + serializeExpr(e) + " AS " + serializeDataType(as) + ")"
    }
    
    def serializeFunctionCall(fc: FunctionCallExpr) =
        fc.name + "(" + fc.params.map(serializeExpr _).mkString(", ") + ")"
    
    def serializeExpr(expr: SqlExpr): String = expr match {
        case v: SqlValue => serializeValue(v)
        case s: CastExpr => serializeCast(s)
        case f: FunctionCallExpr => serializeFunctionCall(f)
    }
    
    // XXX: drop mysql
    def serializeModelColumnProperty(cp: ColumnProperty): Option[String] = cp match {
        case vendor.mysql.MysqlAutoIncrement(true) => Some("AUTO_INCREMENT")
        case vendor.mysql.MysqlAutoIncrement(false) => None
        case Nullability(true) => Some("NULL")
        case Nullability(false) => Some("NOT NULL")
        case DefaultValue(value) => Some("DEFAULT " + serializeExpr(value))
    }
    
    def serializeImportedKeyRule(p: ImportedKeyRule) = p match {
        case ImportedKeyNoAction => "NO ACTION"
        case ImportedKeyCascade => "CASCADE"
        case ImportedKeySetNull => "SET NULL"
        case ImportedKeySetDefault => "SET DEFAULT"
    }
    
    def serializeColumnProperty(cp: ColumnPropertyDecl): Option[String] = cp match {
        case ModelColumnProperty(cp) => serializeModelColumnProperty(cp)
        case InlineUnique => Some("UNIQUE")
        case InlinePrimaryKey => Some("PRIMARY KEY")
        case InlineReferences(References(table, Seq(column), updateRule, deleteRule)) =>
            val words = new ArrayBuffer[String]
            words += "REFERENCES " + serializeName(table) + "(" + serializeName(column) + ")"
            words ++= updateRule.map(p => "ON UPDATE " + serializeImportedKeyRule(p))
            words ++= deleteRule.map(p => "ON DELETE " + serializeImportedKeyRule(p))
            Some(words.mkString(" "))
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
    
    def serializeTableElement(e: TableElement): String = e match {
        case c @ Column(name, dataType, attrs) =>
            serializeName(name) + " " + serializeDataType(dataType) +
                    (if (attrs.isEmpty) ""
                    else " " + attrs.flatMap(cp => serializeColumnProperty(cp, c)).mkString(" "))
        case Index(index) => serializeIndex(index)
        case LikeClause(name) => "LIKE " + serializeName(name)
        case c: Constraint => serializeConstraint(c)
    }
    
    def serializeCreateTable(ct: CreateTableStatement, options: Options): String = {
        val CreateTableStatement(name, ifNotExists, TableElementList(elements), tableOptions) = ct
        def mapTableElement(e: TableElement) =
            serializeTableElement(e)
        val l = elements.map(mapTableElement _).reverse
        val (afterComma, indent) =
            if (options.multiline) ("\n", "  ")
            else (" ", "")
        val lines = (List(l.head) ++ l.drop(1).map(_ + "," + afterComma)).reverse.map(indent + _)
        
        val firstLine = "CREATE TABLE " + serializeName(name) + " ("
        val lastLine = ")" +
            (tableOptions.flatMap(serializeCreateTableTableOption _) match {
                case Seq() => ""
                case l => " " + l.mkString(" ")
            })
        
        (List(firstLine) ++ lines ++ List(lastLine)).mkString(if (options.multiline) "\n" else "")
    }
    
    protected def serializeCreateTableTableOption(opt: TableOption): Option[String] =
        Some(serializeTableOption(opt))
    
    protected def serializeTableOption(opt: TableOption): String =
        throw new MysqlDiffException("unknown table option: "+ opt)
    
    def serializeInsert(is: InsertStatement) = {
        val r = new ArrayBuffer[String]
        r += "INSERT"
        if (is.ignore) r += "IGNORE"
        r += "INTO"
        r += serializeName(is.table)
        if (is.columns.isDefined)
            r += ("(" + is.columns.get.mkString(", ") + ")")
        r += "VALUES"
        r += is.data.map(row => "(" + row.map(serializeExpr _).mkString(", ") + ")").mkString(", ")
        r.mkString(" ")
        // XXX: untested
    }
    
    def serializeDropTable(dt: DropTableStatement) = {
        val DropTableStatement(name, ifExists) = dt
        val words = new ArrayBuffer[String]
        words += "DROP TABLE"
        if (ifExists) words += "IF EXISTS"
        words += serializeName(name)
        words.mkString(" ")
    }
    
    def serializeChangeTable(st: AlterTableStatement) =
        "ALTER TABLE " + serializeName(st.name) + " " +
            st.ops.map(serializeAlterTableOperation(_)).mkString(", ")
    
    def serializeColumnPosition(position: ColumnPosition) = position match {
        case ColumnFirst => "FIRST"
        case ColumnAfter(column) => "AFTER " + serializeName(column)
    }
    
    def serializeAddColumn(ac: AddColumn) = {
        val AddColumn(column, pos) = ac
        val words = new ArrayBuffer[String]
        words += "ADD COLUMN"
        words += serializeTableElement(column)
        words ++= pos.map(serializeColumnPosition _)
        words.mkString(" ")
    }
    
    def serializeChangeColumn(cc: ChangeColumn) = {
        val ChangeColumn(oldName, column, pos) = cc
        val words = new ArrayBuffer[String]
        words += "CHANGE COLUMN"
        words += serializeName(oldName)
        words += serializeColumn(column)
        words ++= pos.map(serializeColumnPosition _)
        words.mkString(" ")
    }
    
    def serializeModifyColumn(mc: ModifyColumn) = {
        val ModifyColumn(column, pos) = mc
        val words = new ArrayBuffer[String]
        words += "MODIFY COLUMN"
        words += serializeColumn(column)
        words ++= pos.map(serializeColumnPosition _)
        words.mkString(" ")
    }
    
    def serializeAlterColumnOperation(op: AlterColumnOperation) = op match {
        case SetNotNull(true) => "SET NOT NULL"
        case SetNotNull(false) => "DROP NOT NULL"
        case SetDefault(Some(value)) => "SET DEFAULT " + serializeExpr(value)
        case SetDefault(None) => "DROP DEFAULT"
    }
    
    def serializeAlterTableOperation(op: Operation) = op match {
        case ac: AddColumn => serializeAddColumn(ac)
        case AddExtra(e) => "ADD " + serializeTableElement(e)
        
        case cc: ChangeColumn => serializeChangeColumn(cc)
        case mc: ModifyColumn => serializeModifyColumn(mc)
        case DropColumn(name) => "DROP COLUMN " + serializeName(name)
        case AlterColumn(name, op) => "ALTER COLUMN " + serializeName(name) + " " + serializeAlterColumnOperation(op)
        
        case DropConstraint(name) => "DROP CONSTRAINT " + serializeName(name)
        case DropPrimaryKey => "DROP PRIMARY KEY"
        case DropIndex(name) => "DROP INDEX " + serializeName(name)
        case DropForeignKey(name) => "DROP FOREIGN KEY " + serializeName(name)
        case DropUniqueKey(name) => "DROP KEY " + serializeName(name)
        
        case ChangeTableOption(o) => serializeTableOption(o)
    }
    
    def serializeDefaultDataType(dataType: DefaultDataType) =
        dataType.name + dataType.length.map("(" + _ + ")").getOrElse("")
   
    def serializeDataType(dataType: DataType) = (dataType: @unchecked) match {
        case dataType: DefaultDataType => serializeDefaultDataType(dataType)
        case NumericDataType(None, None) => "NUMERIC"
        case NumericDataType(Some(precision), None) => "NUMERIC(" + precision + ")"
        case NumericDataType(Some(precision), Some(scale)) => "NUMERIC(" + precision + ", " + scale + ")"
    }
    
    def serializeColumn(model: ColumnModel) =
        serializeTableElement(modelSerializer.serializeColumn(model))
    
    def serializeIndexColumn(ik: IndexColumn) = {
        val words = new ArrayBuffer[String]
        words += serializeName(ik.name) + ik.length.map("(" + _ + ")").getOrElse("")
        if (!ik.asc) words += "DESC"
        words.mkString(" ")
    }
    
    def serializePrimaryKey(pk: PrimaryKeyModel) = {
        val words = new ArrayBuffer[String]
        if (pk.name.isDefined) words += "CONSTRAINT " + serializeName(pk.name.get)
        words += "PRIMARY KEY"
        words += ("(" + pk.columns.map(serializeIndexColumn _).mkString(", ") + ")")
        words.mkString(" ")
    }
    
    // XXX: handle MySQL specific stuff
    def serializeForeignKey(fk: ForeignKeyModel) = {
        val ForeignKeyModel(name, localColumns, externalTable, externalColumns, updateRule, deleteRule) = fk
        val words = new ArrayBuffer[String]
        if (name.isDefined) words += "CONSTRAINT " + serializeName(fk.name.get)
        words += "FOREIGN KEY"
        words += ("(" + localColumns.map(serializeIndexColumn _).mkString(", ") + ")")
        words += "REFERENCES"
        words += serializeName(externalTable)
        words += ("(" + externalColumns.mkString(", ") + ")")
        words ++= updateRule.map(p => "ON UPDATE " + serializeImportedKeyRule(p))
        words ++= deleteRule.map(p => "ON DELETE " + serializeImportedKeyRule(p))
        words.mkString(" ")
    }
    
    def serializeUniqueKey(uk: UniqueKeyModel) = {
        val words = new ArrayBuffer[String]
        if (uk.name.isDefined) words += "CONSTRAINT " + serializeName(uk.name.get)
        words += "UNIQUE (" + uk.columns.map(serializeIndexColumn _).mkString(", ") + ")"
        words.mkString(" ")
    }
    
    def serializeIndex(index: IndexModel) = {
        val words = new ArrayBuffer[String]
        words += "INDEX"
        words ++= index.name.map(serializeName _)
        words += ("(" + index.columns.map(serializeIndexColumn _).mkString(", ") + ")")
        words.mkString(" ")
    }
    
}

object ScriptSerializerTests extends MySpecification {
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
        serialized must_== "DROP TABLE users; /* h */ DROP TABLE users; DROP TABLE users; /* h */ /* h */ DROP TABLE users;"
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
