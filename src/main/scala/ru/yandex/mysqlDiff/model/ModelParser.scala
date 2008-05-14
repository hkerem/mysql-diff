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
                columns += ColumnModel(name, dataType, attrs.withDefaultProperty(Nullability(true)))
            case c.PrimaryKey(pk) => pks += pk
            case c.Index(index) => indexes += index
        }
        
        require(pks.length <= 1)
        
        TableModel(name, columns.toList, pks.firstOption, indexes.toList, ct.options)
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
        val ctc = CreateTableStatement.Column("id", DataType.int,
            new ColumnProperties(List(DefaultValue(NumberValue(0)))))
        val ct = CreateTableStatement("x", false, List(ctc), Nil)
        val t = parseCreateTable(ct)
        val tc = t.column("id")
        tc.properties.find(NullabilityPropertyType) must_== Some(Nullability(true))
    }
}

// vim: set ts=4 sw=4 et:
