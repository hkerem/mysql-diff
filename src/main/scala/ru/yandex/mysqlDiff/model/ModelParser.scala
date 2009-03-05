package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import Implicits._

import script._

import scalax.io._

/**
 * Parse script into model.
 */
case class ModelParser(val context: Context) {
    import context._
    
    def parseModel(text: String): DatabaseModel =
        parseModel(parser.parse(text))
    
    def parseModel(script: Script): DatabaseModel = {
        script.ddlStatements.foldLeft(new DatabaseModel(Nil))((db, stmt) => parseScriptElement(stmt, db))
    }
    
    def parseScriptElement(stmt: DdlStatement, db: DatabaseModel) = stmt match {
        // XXX: handle IF NOT EXISTS
        case ct: CreateTableStatement => db.createTable(parseCreateTable(ct))
        case CreateTableLikeStatement(name, _, like) => db.createTable(db.table(like).withName(name))
    }
    
    def parseCreateTable(ct: CreateTableStatement): TableModel = {
        val c = CreateTableStatement
        
        val name = ct.name
        val columns = new ArrayBuffer[ColumnModel]
        val pks = new ArrayBuffer[PrimaryKeyModel]
        val keys = new ArrayBuffer[KeyModel]
        ct.entries.map {
            case column @ c.Column(name, dataType, attrs) =>
                if (dataType.name == "TIMESTAMP" && column.modelProperties.defaultValue.isEmpty)
                    // because of MySQL-specifc features that are hard to deal with
                    throw new Exception(
                            "TIMESTAMP without DEFAULT value is prohibited, column " + name + ", table " + ct.name)
                
                columns += ColumnModel(name, dataType, column.modelProperties)
                
                attrs foreach {
                    case c.InlinePrimaryKey => pks += PrimaryKeyModel(None, List(column.name))
                    // XXX: other inline properties
                    
                    case c.ModelColumnProperty(_) =>
                }
                
            case c.PrimaryKey(pk) => pks += pk
            case c.Index(index) => keys += index
            case c.ForeignKey(fk) => keys += fk
        }
        
        require(pks.length <= 1)
        
        val pk = pks.firstOption
        
        val columns2 = columns.map {
            c =>
                // XXX: drop here, reasonable only for PK columns
                val defaultAutoincrement = AutoIncrement(false)
                
                val properties = c.properties
                    .withDefaultProperty(defaultAutoincrement)
                
                ColumnModel(c.name, c.dataType, properties)
        }
        
        fixTable(TableModel(name, columns2.toList, pk, keys.toList, ct.options))
    }
    
    protected def fixTable(table: TableModel) = {
        val TableModel(name, columns, pk, keys, options) = table
        TableModel(name, columns.map(fixColumn(_, table)), pk, keys, options)
    }
    
    protected def fixColumn(column: ColumnModel, table: TableModel) =
        column.withDataType(fixDataType(column.dataType, column, table)) match {
            case c if table.isPk(c.name) => fixPkColumn(c)
            case c => fixRegularColumn(c)
        }
    
    protected def fixPkColumn(column: ColumnModel) = {
        var properties = column.properties
        
        // don't know which is better
        properties = properties.overrideProperty(Nullability(false))
        //properties = properties.removeProperty(NullabilityPropertyType)
        
        if (properties.defaultValue == Some(NullValue))
            properties = properties.removeProperty(DefaultValuePropertyType)
        ColumnModel(column.name, column.dataType, properties)
    }
    
    protected def fixRegularColumn(column: ColumnModel) = {
        var properties = column.properties
        properties = properties.withDefaultProperty(Nullability(true))
        properties = properties.withDefaultProperty(DefaultValue(NullValue))
        ColumnModel(column.name, column.dataType, properties)
    }
    
    protected def fixDataType(dataType: DataType, column: ColumnModel, table: TableModel): DataType =
        dataType
    
    def parseCreateTableScript(text: String) =
        parseCreateTable(sqlParserCombinator.parseCreateTableRegular(text))
    
    def main(args: Array[String]) {
        val text = InputStreamResource.file(args(0)).reader.slurp()
        val model = parseModel(text)
        print(ModelSerializer.serializeDatabaseToText(model))
    }
}

class ModelParserTests(context: Context) extends org.specs.Specification {
    import context._
    import modelParser._
    
    "unspecified nullability means nullable" in {
        val ctc = CreateTableStatement.Column("age", dataTypes.int,
            new ColumnProperties(List(DefaultValue(NumberValue(0)))))
        val ct = CreateTableStatement("x", false, List(ctc), Nil)
        val t = parseCreateTable(ct)
        val tc = t.column("age")
        tc.properties.find(NullabilityPropertyType) must_== Some(Nullability(true))
    }
    
    "unspecified DEFAULT VALUE means NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (login VARCHAR(10))")
        val c = t.column("login")
        c.properties.defaultValue must_== Some(NullValue)
    }
    
    "unspecified autoincrement" in {
        val t = parseCreateTableScript("CREATE TABLE user (id INT, login VARCHAR(10), PRIMARY KEY(id))")
        t.column("id").properties.autoIncrement must_== Some(false)
        //t.column("login").properties.autoIncrement must_== None
    }
    
    "PK is automatically NOT NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT, name VARCHAR(10), PRIMARY KEY(id))")
        val idColumn = t.column("id")
        idColumn.properties.find(NullabilityPropertyType) must_== Some(Nullability(false))
    }
    
    "inline PK" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT PRIMARY KEY, login VARCHAR(10))")
        t.columns.length must_== 2
        t.primaryKey.get.columns must beLike { case Seq("id") => true; case _ => false }
    }
    
    /*
    "MySQL TIMESTAMP is DEFAULT NOW()" in {
        val ct = sqlParserCombinator.parseCreateTable(
            "CREATE TABLE files (created TIMESTAMP)")
        val t = parseCreateTable(ct)
        val c = t.column("created")
        c.properties.defaultValue must_== Some(NowValue)
    }
    */
    
    "Prohibit TIMESTAMP without DEFAULT value" in {
        val ct = sqlParserCombinator.parseCreateTableRegular(
            "CREATE TABLE x (a TIMESTAMP)")
        try {
            val t = parseCreateTable(ct)
            fail("table should not be allowed, created " + t)
        } catch {
            case e: Exception if e.getMessage contains "prohibited" =>
        }
    }
    
}

object ModelParserTests extends ModelParserTests(Environment.defaultContext)

// vim: set ts=4 sw=4 et:
