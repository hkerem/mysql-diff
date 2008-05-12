package ru.yandex.mysqlDiff.script

import model._

import scala.collection.mutable.ArrayBuffer

object ScriptSerializer {
    /** Serializer options */
    abstract class Options {
        def stmtJoin: String
        def scriptTail: String = stmtJoin
        def indent = ""
        def afterComma = ""
    }
    
    object Options {
        object multiline extends Options {
            override def stmtJoin = "\n"
            override def indent = "    "
        }
        
        object singleline extends Options {
            override def stmtJoin = " "
            override def scriptTail = ""
            override def afterComma = " "
        }
    }
    
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
    
    def serializeStatement(stmt: ScriptStatement, options: Options): String = stmt match {
        case st: CreateTableStatement => serializeCreateTable(st, options)
        case DropTableStatement(n) => serializeDropTable(n)
        case st: AlterTableStatement => serializeChangeTable(st)
    }
    
    def serializeValue(value: SqlValue) = value match {
        case NullValue => "NULL"
        case NumberValue(number) => number.toString
        case StringValue(string) => "'" + string + "'" // XXX: escape
        case NowValue => "NOW()"
    }
    
    def serializeColumnProperty(cp: CreateTableStatement.ColumnProperty) = cp match {
        case CreateTableStatement.Nullable(true) => "NULL"
        case CreateTableStatement.Nullable(false) => "NOT NULL"
        case CreateTableStatement.DefaultValue(value) => "DEFAULT " + serializeValue(value)
        case CreateTableStatement.AutoIncrement => "AUTO_INCREMENT"
    }
    
    def serializeTableEntry(e: CreateTableStatement.Entry): String = e match {
        case CreateTableStatement.Column(name, dataType, attrs) =>
            name + " " + serializeDataType(dataType) +
                    (if (attrs.isEmpty) ""
                    else " " + attrs.map(serializeColumnProperty _).mkString(" "))
        case CreateTableStatement.PrimaryKey(pk) => serializePrimaryKey(pk)
        case CreateTableStatement.Index(index) => serializeIndex(index)
    }
    
    def serializeCreateTable(t: CreateTableStatement, options: Options): String = {
        val l = t.entries.map(serializeTableEntry _).reverse
        val lines = (List(l.first) ++ l.drop(1).map(_ + "," + options.afterComma)).reverse.map(options.indent + _)
        (List("CREATE TABLE " + t.name + " (") ++ lines ++ List(")")).mkString(options.stmtJoin)
    }
    
    def serializeDropTable(tableName: String) =
        "DROP TABLE " + tableName
    
    def serializeChangeTable(st: AlterTableStatement) =
        "ALTER TABLE " + st.tableName + " " +
            st.ops.map(serializeAlterTableOperation(_)).mkString(", ")
    
    def serializeAlterTableOperation(op: AlterTableStatement.Operation) = {
        import AlterTableStatement._
        op match {
            case AddColumn(column) => "ADD COLUMN " + serializeColumn(column)
            case ChangeColumn(oldName, column) => "CHANGE COLUMN " + oldName + " " + serializeColumn(column)
            case ModifyColumn(column) => "MODIFY COLUMN " + serializeColumn(column)
            case DropColumn(name) => "DROP COLUMN " + name
            
            case DropPrimaryKey => "DROP PRIMARY KEY"
            case AddPrimaryKey(pk) => "ADD " + serializePrimaryKey(pk)
            
            case DropIndex(name) => "DROP INDEX " + name
            case AddIndex(index) => "ADD " + serializeIndex(index)
        }
    }
   
    def serializeDataType(dataType: DataType) = {
        // XXX: dirty
        var charset = ""
        if (dataType.characterSet.isDefined) charset = " CHARACTER SET " + dataType.characterSet
        var collate = ""
        if (dataType.collate.isDefined) collate = " COLLATE " + dataType.collate
        var unsigned = ""
        if (dataType.isUnsigned) unsigned = " UNSIGNED"
        var zerofill = ""
        if (dataType.isZerofill) zerofill = " ZEROFILL"
        var size = ""
        if (dataType.length.isDefined) size = "(" + dataType.length.get + ")"
        val result = dataType.name + size + charset + collate + unsigned + zerofill
        result.trim
    }
    
    // XXX: move to ModelSerializer
    def serializeColumn(model: ColumnModel) = {
        val attributes = new ArrayBuffer[String]
        
        attributes += (if (model.isNotNull) "NOT NULL" else "NULL")
        if (model.isAutoIncrement) attributes += "AUTOINCREMENT"
        if (model.defaultValue.isDefined) attributes += "DEFAULT " + model.defaultValue.get
        if (model.comment.isDefined) attributes += "COMMENT " + model.comment.get // XXX: lies
        
        model.name + " " + serializeDataType(model.dataType) +
                (if (attributes.isEmpty) "" else " " + attributes.mkString(" "))
    }
    
    def serializePrimaryKey(pk: PrimaryKey) = {
        val words = new ArrayBuffer[String]
        words += "PRIMARY KEY"
        words ++= pk.name
        words += ("(" + pk.columns.mkString(", ") + ")")
        words.mkString(" ")
    }
    
    def serializeIndex(index: IndexModel) = {
        val words = new ArrayBuffer[String]
        if (index.isUnique) words += "UNIQUE"
        words += "INDEX"
        words ++= index.name
        words += ("(" + index.columns.mkString(", ") + ")")
        words.mkString(" ")
    }
        
}

import scalax.testing._

object ScriptSerializerTest extends TestSuite("ScriptSerializerTest") {
    import ScriptSerializer._
    
    "serialize semi singleline" is {
        val dt = DropTableStatement("users")
        val c = CommentElement("/* h */")
        
        val script = List(dt, c, dt, dt, c, c, dt)
        
        val options = Options.singleline
        
        val serialized = serialize(script, options)
        
        //println("'" + serialized + "'")
        assert(serialized == "DROP TABLE users; /* h */ DROP TABLE users; DROP TABLE users; /* h */ /* h */ DROP TABLE users;")
    }
}

// vim: set ts=4 sw=4 et:
