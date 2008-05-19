package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._

import scalax.io._

object ModelParser {
    def parseModel(text: String): DatabaseModel =
        parseModel(parser.Parser.parse(text))
    
    def parseModel(script: Script): DatabaseModel =
        new DatabaseModel("db", script.ddlStatements.map(parseScriptElement _))
    
    def parseScriptElement(stmt: DdlStatement): TableModel = stmt match {
        case ct: CreateTableStatement => parseCreateTable(ct)
        case _ => throw new IllegalArgumentException
    }
    
    def parseCreateTable(ct: CreateTableStatement): TableModel = {
        val c = CreateTableStatement
        
        val name = ct.name
        val columns = new ArrayBuffer[ColumnModel]
        val pks = new ArrayBuffer[PrimaryKey]
        val indexes = new ArrayBuffer[IndexModel]
        ct.entries.map {
            case c.Column(name, dataType, attrs) =>
                if (dataType.name == "TIMESTAMP" && attrs.defaultValue.isEmpty)
                    // because of MySQL-specifc features that are hard to deal with
                    throw new Exception("TIMESTAMP without DEFAULT value is prohibited")
                columns += ColumnModel(name, dataType, attrs)
            case c.PrimaryKey(pk) => pks += pk
            case c.Index(index) => indexes += index
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
        
        TableModel(name, columns2.toList, pk, indexes.toList, ct.options)
    }
    
    def main(args: Array[String]) {
        val text = InputStreamResource.file(args(0)).reader.slurp()
        val model = parseModel(text)
        print(ModelSerializer.serializeDatabaseToText(model))
    }
}

object ModelParserTests extends org.specs.Specification {
    import ModelParser._
    
    "unspecified nullability means nullable" in {
        val ctc = CreateTableStatement.Column("age", DataType.int,
            new ColumnProperties(List(DefaultValue(NumberValue(0)))))
        val ct = CreateTableStatement("x", false, List(ctc), Nil)
        val t = parseCreateTable(ct)
        val tc = t.column("age")
        tc.properties.find(NullabilityPropertyType) must_== Some(Nullability(true))
    }
    
    "unspecified DEFAULT VALUE means NULL" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE users (login VARCHAR(10))")
        val t = parseCreateTable(ct)
        val c = t.column("login")
        c.properties.defaultValue must_== Some(NullValue)
    }
    
    "unspecified autoincrement" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE user (id INT, login VARCHAR(10), PRIMARY KEY(id))")
        val t = parseCreateTable(ct)
        t.column("id").properties.autoIncrement must_== Some(false)
        //t.column("login").properties.autoIncrement must_== None
    }
    
    "PK is automatically NOT NULL" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE users (id INT, name VARCHAR(10), PRIMARY KEY(id))")
        val t = parseCreateTable(ct)
        val idColumn = t.column("id")
        idColumn.properties.find(NullabilityPropertyType) must_== Some(Nullability(false))
    }
    
    /*
    "MySQL TIMESTAMP is DEFAULT NOW()" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE files (created TIMESTAMP)")
        val t = parseCreateTable(ct)
        val c = t.column("created")
        c.properties.defaultValue must_== Some(NowValue)
    }
    */
    
    "Prohibit TIMESTAMP without DEFAULT value" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
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
