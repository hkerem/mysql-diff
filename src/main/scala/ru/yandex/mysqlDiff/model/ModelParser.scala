package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._
import script.Implicits._

import scalax.io._

case class ModelParser(val context: Context) {
    import context._
    
    def parseModel(text: String): DatabaseModel =
        parseModel(parser.parse(text))
    
    def parseModel(script: Script): DatabaseModel =
        new DatabaseModel(script.ddlStatements.map(parseScriptElement _))
    
    def parseScriptElement(stmt: DdlStatement): TableModel = stmt match {
        case ct: CreateTableStatement => parseCreateTable(ct)
        case _ => throw new IllegalArgumentException
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
        
        def pkContainsColumn(c: String) =
            pk.exists(_.columns.exists(_ == c))
        
        val columns2 = columns.map {
            c =>
                val defaultNullability = Nullability(!pkContainsColumn(c.name))
                val defaultAutoincrement = AutoIncrement(false)
                
                val properties = c.properties
                    .withDefaultProperty(defaultNullability)
                    .withDefaultProperty(DefaultValue(NullValue))
                    .withDefaultProperty(defaultAutoincrement)
                
                ColumnModel(c.name, c.dataType, properties)
        }
        
        TableModel(name, columns2.toList, pk, keys.toList, ct.options)
    }
    
    def parseCreateTableScript(text: String) =
        parseCreateTable(sqlParserCombinator.parseCreateTable(text))
    
    def main(args: Array[String]) {
        val text = InputStreamResource.file(args(0)).reader.slurp()
        val model = parseModel(text)
        print(ModelSerializer.serializeDatabaseToText(model))
    }
}

object ModelParserTests extends org.specs.Specification {
    import Environment.defaultContext._
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
        val ct = sqlParserCombinator.parseCreateTable(
            "CREATE TABLE x (a TIMESTAMP)")
        try {
            val t = parseCreateTable(ct)
            fail("table should not be allowed, created " + t)
        } catch {
            case e: Exception if e.getMessage contains "prohibited" =>
        }
    }
}

// vim: set ts=4 sw=4 et:
