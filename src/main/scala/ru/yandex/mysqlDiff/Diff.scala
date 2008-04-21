package ru.yandex.mysqlDiff

import ru.yandex.mysqlDiff.diff.simple._
import scala.io._
import java.io.File

object Diff {
    val helpBanner: String = "MySql diff maker\n" + 
      "How to use:\n" +
      "mysqlDiff file_name_from file_name_to"
      
    def main(args: Array[String]) {
        if (args.length < 2) {
          Console.println(helpBanner);
          return
        }
        val from = new File(args(0))
        val to = new File(args(1))
        
        if (!from.isFile) {
          Console.println("\"From\" file is not a file.")
        }

        if (!to.isFile) {
          Console.println("\"to\" file is not a file.")
        }
        var fromStr = ""
        Source.fromFile(from).getLines.foreach(x => {fromStr = fromStr + x})

        var toStr = ""
        Source.fromFile(to).getLines.foreach(x => {toStr = toStr + x})
        
        val fromdb = SimpleTextHarvester.parse(fromStr);
        val todb = SimpleTextHarvester.parse(toStr);
        
        Console.println("-- start diff script from file " + from  + " to file " + to + "\n");
        val dbDiffMaker = new DatabaseDiffMaker(fromdb, todb);
        dbDiffMaker.doDiff(x => {
          val outputScript = SimpleScriptBuilder.getString(x);
          Console.println(outputScript)
          true
        });
        Console.println("-- end of diff script from file " + from  + " to file " + to + "\n");
    }
}
