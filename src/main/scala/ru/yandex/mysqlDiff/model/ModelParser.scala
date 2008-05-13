package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import script._

object ModelParser {
    def parseModel(text: String): DatabaseModel =
        parseModel(parser.Parser.parse(text))
    
    def parseModel(script: Script): DatabaseModel =
        new DatabaseModel("db", script.stmts.flatMap(parseScriptElement _))
    
    def parseScriptElement(stmt: ScriptElement): Option[TableModel] = stmt match {
        case _: CommentElement => None
        case ct: CreateTableStatement => Some(parseCreateTable(ct))
    }
    
    def parseCreateTable(ct: CreateTableStatement): TableModel = {
        val c = CreateTableStatement
        
        val name = ct.name
        val columns = new ArrayBuffer[ColumnModel]
        val pks = new ArrayBuffer[PrimaryKey]
        val indexes = new ArrayBuffer[IndexModel]
        ct.entries.map {
            case c.Column(name, dataType, attrs) =>
                columns += ColumnModel(name, dataType, attrs)
            case c.PrimaryKey(pk) => pks += pk
            case c.Index(index) => indexes += index
        }
        
        require(pks.length <= 1)
        
        new TableModel(name, columns.toList, pks.firstOption, indexes.toList)
    }
}

// vim: set ts=4 sw=4 et:
