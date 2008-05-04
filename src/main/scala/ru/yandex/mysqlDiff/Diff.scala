package ru.yandex.mysqlDiff


import java.io.File
import scalax.io._

import diff._
import model._
import script._
import jdbc._

object Diff {
    val helpBanner: String = "MySQL diff maker\n" +
        "How to use:\n" +
        "mysqlDiff.sh from_file|from_jdbc_url to_file|to_jdbc_url"

    def main(args: Array[String]) {
        if (args.length != 2) {
            System.err.println(helpBanner)
            System.exit(1)
        } else {
            val fromArgs = args(0)
            val toArgs = args(1)
            if (fromArgs == null || fromArgs.trim == "" || toArgs == null|| toArgs.trim == "") {
                System.err.println(helpBanner)
                System.exit(1)
            } else {
                try {
                    var fromdb = getModelFromArgsLine(args(0))
                    var todb = getModelFromArgsLine(args(1))
                    Console.println("-- Start diff script from " + fromArgs  + " to " + toArgs + "\n")

                    val dbDiff = DatabaseDiffMaker.doDiff(fromdb, todb)

                    val script = DiffSerializer.serialize(fromdb, todb, dbDiff)

                    println(script)

                    Console.println("-- End of diff script from " + fromArgs  + " to " + toArgs)
                } catch {
                    case e: MysqlDiffException => {
                        System.err.println("An error while diff building")
                        System.err.println(e.message)
                        System.exit(2)
                    }
                }
            }
        }
    }

    def getModelFromArgsLine(arg: String): DatabaseModel = {
        if (arg.toLowerCase.startsWith("jdbc:"))
            JdbcModelExtractor.parse(arg.trim)
        else {
            val sourceF = new File(arg)
            if (!sourceF.isFile)  throw new MysqlDiffException("\"" + arg + "\" file is not a file.")
            var str = new FileExtras(sourceF).slurp
            ScriptParser.parseModel(str)
        }
    }

}
