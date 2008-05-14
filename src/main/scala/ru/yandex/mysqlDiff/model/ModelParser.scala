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
                val newAttrs =
                    // MySQL specific
                    if (dataType.name == "TIMESTAMP") attrs.withDefaultProperty(DefaultValue(NowValue))
                    else attrs
                columns += ColumnModel(name, dataType, newAttrs)
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
                ColumnModel(c.name, c.dataType,
                    c.properties
                        .withDefaultProperty(defaultNullability)
                        .withDefaultProperty(DefaultValue(NullValue)))
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
    
    "PK is automatically NOT NULL" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE users (id INT, name VARCHAR(10), PRIMARY KEY(id))")
        val t = parseCreateTable(ct)
        val idColumn = t.column("id")
        idColumn.properties.find(NullabilityPropertyType) must_== Some(Nullability(false))
    }
    
    "unspecified DEFAULT VALUE means NULL" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE users (login VARCHAR(10))")
        val t = parseCreateTable(ct)
        val c = t.column("login")
        c.properties.defaultValue must_== Some(NullValue)
    }
    
    "MySQL TIMESTAMP is DEFAULT NOW()" in {
        val ct = script.parser.SqlParserCombinator.parseCreateTable(
            "CREATE TABLE files (created TIMESTAMP)")
        val t = parseCreateTable(ct)
        val c = t.column("created")
        c.properties.defaultValue must_== Some(NowValue)
    }
}

// vim: set ts=4 sw=4 et:
