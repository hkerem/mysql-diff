package ru.yandex.mysqlDiff.script

import scala.collection.mutable.ArrayBuffer

import model._

object ScriptSerializer {
    /** Serializer options */
    case class Options(val indent: String, nl: String, afterComma: String)
    
    object Options {
        def multiline = Options("    ", "\n", "")
        def singleline = Options("", "", " ")
    }
    
    def serialize(stmts: Seq[ScriptElement], options: Options): String = {
        val sb = new StringBuilder
        var needSemi = false
        var first = true
        for (stmt <- stmts) {
            if (needSemi) {
                sb append ";"
                sb append options.afterComma
            }
            
            if (!first) {
                // XXX: care about "--" comments
                sb append options.nl
            }
            first = false
            
            sb append serialize(stmt, options)
            needSemi = stmt match {
                case c: CommentElement => false
                case _ => true
            }
        }
        sb.toString
    }

    def serialize(stmt: ScriptElement, options: Options): String = stmt match {
        case s: ScriptStatement => serializeStatement(s, options)
        case Unparsed(q) => q
        case CommentElement(c) => c
    }
    
    def serializeStatement(stmt: ScriptStatement, options: Options): String = stmt match {
        case CreateTableStatement(t) => serializeCreateTable(t, options)
        case DropTableStatement(n) => serializeDropTable(n)
        case st: AlterTableStatement => serializeAlterTable(st)
    }
    
    def serializeCreateTable(createTable: CreateTableStatement, options: Options): String =
        serializeCreateTable(createTable.table, options)
    
    def serializeCreateTable(table: TableModel, options: Options): String = {
        val l = (
                table.columns.map(serializeColumn _) ++
                table.primaryKey.map(serializePrimaryKey _) ++
                table.keys.map(serializeIndex _)
            ).reverse
        val lines = (List(l.first) ++ l.drop(1).map(_ + "," + options.afterComma)).reverse.map(options.indent + _)
        
        (List("CREATE TABLE " + table.name + "(") ++ lines ++ List(")")).mkString(options.nl)
    }
    
    def serializeDropTable(tableName: String) =
        "DROP TABLE " + tableName
    
    def serializeAlterTable(st: AlterTableStatement) =
        "ALTER TABLE " + st.tableName + " " +
            st.ops.map(serializeAlterTableOperation(_)).mkString(", ")
    
    def serializeAlterTableOperation(op: AlterTableStatement.Operation) = {
        import AlterTableStatement._
        op match {
            case AddColumn(column) => "ADD COLUMN " + serializeColumn(column)
            case ChangeColumn(oldName, column) => "CHANGE COLUMN " + oldName + " " + serializeColumn(column)
            case ModifyColumn(column) => "MODIFY COLUMN " + serializeColumn(column)
            case DropColumn(name) => "DROP COLUMN " + name
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
    
    def serializeColumn(model: ColumnModel) = {
        val attributes = new ArrayBuffer[String]
        
        attributes += (if (model.isNotNull) "NOT NULL" else "NULL")
        if (model.isAutoIncrement) attributes += "AUTOINCREMENT"
        if (model.defaultValue.isDefined) attributes += "DEFAULT " + model.defaultValue.get
        if (model.comment.isDefined) attributes += "COMMENT " + model.comment.get // XXX: lies
        
        model.name + " " + serializeDataType(model.dataType) +
                (if (attributes.isEmpty) "" else " " + attributes.mkString(" "))
    }
    
    def serializePrimaryKey(pk: PrimaryKey) =
        "PRIMARY KEY (" + pk.columns.mkString(", ") + ")"
    
    def serializeIndex(index: IndexModel) =
        (if (index.isUnique) "UNIQUE " else "") + "INDEX " + index.name + "(" + index.columns.mkString(", ") + ")"
}

import scalax.testing._

object ScriptSerializerTest extends TestSuite("ScriptSerializerTest") {
    import ScriptSerializer._
    
    "serializeCreateTable" is {
        val idColumn = ColumnModel("id", DataType.int())
        val nameColumn = ColumnModel("name", DataType.varchar(100))
        val t = TableModel("users", List(idColumn, nameColumn))
        
        val script = serializeCreateTable(t, Options("  ", "\n", "--"))
        assert(script.matches("CREATE TABLE users\\(\n  id INT.*,--\n  name VARCHAR\\(100\\).*\n\\)"))
    }
    
    "serialize semi" is {
        val dt = DropTableStatement("users")
        val c = CommentElement("/* h */")
        
        val script = List(dt, c, dt, dt, c, c, dt)
        
        val options = Options("", " ", "")
        
        val serialized = serialize(script, options)
        
        assert(serialized == "DROP TABLE users; /* h */ DROP TABLE users; DROP TABLE users; /* h */ /* h */ DROP TABLE users")
    }
}

// vim: set ts=4 sw=4 et:
