package ru.yandex.mysqlDiff


import java.io.File
import scala.io._

import diff._
import model._
import script._
import jdbc._

object Diff {
    val helpBanner: String = "MySQL diff maker\n" +
        "How to use:\n" +
        "mysqlDiff.sh file_name_from|jdbc:jdbc_url_to_source  file_name_to|jdbc:jdbc_url_to_destination\n"

    def main(args: Array[String]) = {
        if (args.length < 2) {
            Console.println(helpBanner)
        } else {

            val fromArgs = args(0)
            val toArgs = args(1)
            if (fromArgs == null || fromArgs.trim.equals("") || toArgs == null|| toArgs.trim.equals("")) {
                Console.println(helpBanner)
            } else {

                var fromdb = getModelFromArgsLine(args(0))
                var todb = getModelFromArgsLine(args(1))

                Console.println("-- start diff script from " + fromArgs  + " to " + toArgs + "\n");

                val dbDiff = DatabaseDiffMaker.doDiff(fromdb, todb)
        
                val script = DiffSerializer.serialize(fromdb, todb, dbDiff)

                println(script)

                Console.println("-- end of diff script from " + fromArgs  + " to " + toArgs + "\n");
            }
        }
    }

    def getModelFromArgsLine(arg: String): DatabaseModel = {
        if (arg.toLowerCase.startsWith("jdbc:")) JdbcModelExtractor.parse(arg.trim)
            else {
                val sourceF = new File(arg)
                if (!sourceF.isFile)  throw new RuntimeException("\"" + arg + "\" file is not a file.")
                var str = ""
                Source.fromFile(sourceF).getLines.foreach(x => {str = str + x})
                ScriptParser.parseModel(str)
            }
    }

}
