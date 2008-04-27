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
	val fromArgs = args(0)
	val toArgs = args(1)

	if (fromArgs == null || fromArgs.trim.equals("")) {
	  Console.println(helpBanner)
	  return
	}
	if (toArgs == null|| toArgs.trim.equals("")) {
	  Console.println(helpBanner)
          return
	}
	
        var fromStr = ""
	var toStr = ""


        if (!fromArgs.toUpperCase.startsWith("JDBC")) {
          val from = new File(args(0))
          if (!from.isFile) {
	   Console.println("\"From\" file is not a file.")
	  }
	  Source.fromFile(from).getLines.foreach(x => {fromStr = fromStr + x})
	}


	
	
	if (!toArgs.toUpperCase.startsWith("JDBC")) {
	  val to = new File(args(1))
          if (!to.isFile) {
            Console.println("\"to\" file is not a file.")
          }
          Source.fromFile(to).getLines.foreach(x => {toStr = toStr + x})
        }        
 
        val fromdb = SimpleTextHarvester.parse(fromStr);
        val todb = SimpleTextHarvester.parse(toStr);
        
        Console.println("-- start diff script from " + fromArgs  + " to " + toArgs + "\n");
        val dbDiffMaker = new DatabaseDiffMaker(fromdb, todb);
        dbDiffMaker.doDiff(x => {
          val outputScript = SimpleScriptBuilder.getString(x);
          Console.println(outputScript)
          true
        });
        Console.println("-- end of diff script from " + fromArgs  + " to " + toArgs + "\n");
    }
}
