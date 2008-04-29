package ru.yandex.mysqlDiff


import java.io.File
import scala.io._

import ru.yandex.mysqlDiff.diff.simple._
import ru.yandex.mysqlDiff.model._

object Diff {
    val helpBanner: String = "MySql diff maker\n" +
        "How to use:\n" +
        "mysqlDiff.sh file_name_from|jdbc:jdbc_url_to_source  file_name_to|jdbc:jdbc_url_to_destination\n"

    def main(args: Array[String]) {
        if (args.length < 2) {
            Console.println(helpBanner)
            return
        }

        val fromArgs = args(0)
        val toArgs = args(1)

        if (fromArgs == null || fromArgs.trim.equals("") || toArgs == null|| toArgs.trim.equals("")) {
            Console.println(helpBanner)
            return
        }

        var fromdb: DatabaseModel = null
        var todb: DatabaseModel = null


        if (fromArgs.toLowerCase.startsWith("jdbc:")) {
            val fromUrl = fromArgs.trim
            fromdb = SimpleJdbcHarvester.parse(fromUrl)
        } else {
            val from = new File(args(0))
            if (!from.isFile) {
                Console.println("\"From\" file is not a file.")
                return
            }
            var fromStr = ""
            Source.fromFile(from).getLines.foreach(x => {fromStr = fromStr + x})
            fromdb = SimpleTextParser.parse(fromStr);
        }

        if (toArgs.toLowerCase.startsWith("jdbc:")) {
            val toUrl = toArgs.trim
            todb = SimpleJdbcHarvester.parse(toUrl)
        } else {
            val to = new File(args(1))
            if (!to.isFile) {
                Console.println("\"to\" file is not a file.")
                return;
            }
            var toStr = ""
            Source.fromFile(to).getLines.foreach(x => {toStr = toStr + x})
            todb = SimpleTextParser.parse(toStr);
        }

        Console.println("-- start diff script from " + fromArgs  + " to " + toArgs + "\n");
        val dbDiffMaker = new DatabaseDiffMaker(fromdb, todb);
        dbDiffMaker.doDiff(x => {
            val outputScript = SimpleScriptBuilder.getString(x);
            Console.println(outputScript)
            true
        })

        Console.println("-- end of diff script from " + fromArgs  + " to " + toArgs + "\n");
    }
}
