package ru.yandex.mysqlDiff


import java.io.File
import scalax.io._

import diff._
import model._
import script._
import jdbc._

object Utils {
    
    def getModelFromArgsLine(arg: String): DatabaseModel = {
        if (arg.startsWith("jdbc:"))
            JdbcModelExtractor.parse(arg)
        else {
            var str = ReaderResource.file(arg).slurp
            model.ModelParser.parseModel(str)
        }
    }
    
    def getModelFromArgsLine(arg: String, table: String) = {
        if (arg.startsWith("jdbc:"))
            new DatabaseModel(List(JdbcModelExtractor.parseTable(table, arg)))
        else {
            var str = ReaderResource.file(arg).slurp
            val tableModel = model.ModelParser.parseModel(str).declarations.filter(_.name == table).first
            new DatabaseModel(List(tableModel))
        }
    }
}

import Utils._

abstract class MainSupport {
    val helpBanner: String
    
    def usage() {
        Console.err.println(helpBanner)
    }
    
    def main(args: Array[String])
}

object Diff extends MainSupport {
    override val helpBanner = "mysqlDiff.sh from_file|from_jdbc_url to_file|to_jdbc_url"

    override def main(args: Array[String]) {
        
        val (fromdb, todb) = args match {
            case Seq(from, to) =>
                (getModelFromArgsLine(from), getModelFromArgsLine(to))
            case Seq(from, to, table) =>
                (getModelFromArgsLine(from, table), getModelFromArgsLine(to, table))
            case _ =>
                usage()
                exit(1)
        }
        
        val dbDiff = DiffMaker.compareDatabases(fromdb, todb)

        val script = DiffSerializer.serialize(fromdb, todb, dbDiff)

        print(script)

    }
}

object Dump extends MainSupport {
    override val helpBanner = "$0 file|jdbc_url"
    
    override def main(args: Array[String]) {
        val db = args match {
            case Seq(db) => getModelFromArgsLine(db)
            case Seq(db, table) => getModelFromArgsLine(db, table)
            case _ =>
                usage()
                exit(1)
        }
        
        print(ModelSerializer.serializeDatabaseToText(db))
    }
}

// meta main
/*
object Main extends MainSupport {
    override val helpBanner = "$0 command $@"
    
    override def main(args: Array
}
*/

// vim: set ts=4 sw=4 et: