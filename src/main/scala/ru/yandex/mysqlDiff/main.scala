package ru.yandex.mysqlDiff


import java.io.File
import scalax.io._

import diff._
import model._
import script._
import jdbc._
import util._
import util.getopt._

import Implicits._

class Utils(context: Context) {
    import context._
    
    def getModelFromArgsLine(arg: String): DatabaseModel = {
        if (arg.startsWith("jdbc:"))
            connectedContext(LiteDataSource.driverManager(arg)).jdbcModelExtractor.extract()
        else {
            var str = ReaderResource.file(arg).slurp
            modelParser.parseModel(str)
        }
    }
    
    def getModelFromArgsLine(arg: String, table: String): DatabaseModel = {
        if (arg.startsWith("jdbc:")) {
            val jdbcModelExtractor = connectedContext(LiteDataSource.driverManager(arg)).jdbcModelExtractor
            new DatabaseModel(Seq(jdbcModelExtractor.extractTable(table)))
        } else {
            var str = ReaderResource.file(arg).slurp
            val tableModel = modelParser.parseModel(str).decls.filter(_.name == table).first
            new DatabaseModel(Seq(tableModel))
        }
    }
}

abstract class MainSupport {
    val helpBanner: String
    
    val verboseOpt = getopt.Opt("verbose", false)
    val dbenvOpt = getopt.Opt("dbenv", true)
    val versionOpt = getopt.Opt("version", false)
    
    import Console.err
    
    def usage() {
        err.println(helpBanner)
    }
    
    private def md5(isf: => java.io.InputStream) = {
        val md = java.security.MessageDigest.getInstance("MD5")
        val is = isf
        try {
            var done = false
            while (!done) {
                val buffer = new Array[Byte](0x1000)
                val r = is.read(buffer)
                if (r >= 0) {
                    md.update(buffer, 0, r)
                } else {
                    done = true
                }
            }
            md.digest
        } finally {
            is.close()
        }
    }
    
    private def hex(bs: Array[Byte]) =
        (bs map { b => String.format("%02x", java.lang.Byte.valueOf(b)) }).mkString
    
    private def printCl(name: String, cl: ClassLoader) {
        err.println(name + ": " + cl)
        cl match {
            case cl: java.net.URLClassLoader =>
                for (u <- cl.getURLs) {
                    val r = try {
                        hex(md5(u.openStream))
                    } catch {
                        case e => "failed to get md5: " + e
                    }
                    err.println("    " + u + " (" + r + ")")
                }
        }
        if (cl != null && cl.getParent != null)
            printCl("parent", cl.getParent)
    }
    
    def debug() {
        err.println("main class: " + this.getClass.getName)
        printCl("context classloader", Thread.currentThread.getContextClassLoader)
        printCl(this.getClass.getName + ".classloader", this.getClass.getClassLoader)
        printCl("system classloader", ClassLoader.getSystemClassLoader)
    }
    
    def main(args: Array[String]): Unit = {
        args match {
            case Seq("--debugenv") =>
                debug()
                exit(0)
            case Seq("--version") =>
                println(MysqlDiffVersion.version)
                exit(0)
            case _ =>
        }
    }
}

object Diff extends MainSupport {
    override val helpBanner = "mysqlDiff.sh from_file|from_jdbc_url to_file|to_jdbc_url"

    override def main(args: Array[String]): Unit = {
        super.main(args)
        
        var verbose = false
        var dbenv = "mysql"
        
        val getopt.Result(options, restArgs) = getopt.Options(Seq(verboseOpt, dbenvOpt)).parse(args)
        for ((opt, value) <- options) {
            opt.name match {
                case "verbose" => verbose = true
                case "dbenv" => dbenv = value
            }
        }
        
        val context = Environment.context(dbenv)
        import context._
        val utils = new Utils(context)
        import utils._
        
        val (fromdb, todb, descr) = restArgs match {
            case Seq(from, to) =>
                (getModelFromArgsLine(from), getModelFromArgsLine(to),
                        "diff from %s to %s" % (from, to))
            case Seq(from, to, table) =>
                (getModelFromArgsLine(from, table), getModelFromArgsLine(to, table),
                        "diff of table %s from %s to %s" % (table, from, to))
            case _ =>
                usage()
                exit(1)
        }
        
        val dbDiff = diffMaker.compareDatabases(fromdb, todb)
        
        if (dbDiff.declDiff.isEmpty) {
            if (verbose)
                println("-- "+ descr +" is empty")
        } else {
            if (verbose) {
                println("-- " + descr +":")
                for (d <- dbDiff.declDiff)
                    println("-- "+ d)
            }
            
            val script = diffSerializer.serialize(fromdb, todb, dbDiff)

            print(script)
        }


    }
}

object Dump extends MainSupport {
    override val helpBanner = "$0 file|jdbc_url"
    
    override def main(args: Array[String]): Unit = {
        super.main(args)
        
        var verboseValue = false
        var dbenv: Option[String] = None
        
        val getopt.Result(options, restArgs) = getopt.Options(Seq(verboseOpt, dbenvOpt)).parse(args)
        for ((opt, value) <- options) {
            opt.name match {
                case "verbose" => verboseValue = true
                case "dbenv" => dbenv = Some(value)
            }
        }
        
        val (dbRef, afterDb) = restArgs match {
            case Seq(db, afterDb @_*) => (db, afterDb)
            case _ =>
                usage()
                exit(1)
        }
        
        val context =
            if (dbenv.isDefined) Environment.context(dbenv.get)
            else if (dbRef startsWith "jdbc:") Environment.contextByUrl(dbRef)
            else Environment.defaultContext
        
        import context._
        val utils = new Utils(context)
        import utils._
        
        val db = afterDb match {
            case Seq() => getModelFromArgsLine(dbRef)
            case Seq(table) => getModelFromArgsLine(dbRef, table)
            case _ =>
                usage()
                exit(1)
        }
        
        object serializeOptions extends ScriptSerializer.Options.Multiline {
            override def verbose = verboseValue
        }
        
        //print(ModelSerializer.serializeDatabaseToText(db))
        print(scriptSerializer.serialize(modelSerializer.serializeDatabase(db), serializeOptions))
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
