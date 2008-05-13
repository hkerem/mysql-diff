package ru.yandex.mysqlDiff.jdbc

import scalax.control.ManagedResource

import java.sql.DriverManager

object JdbcUtils {
    Class.forName("com.mysql.jdbc.Driver")
    
    def connection(jdbcUrl: String) = ManagedResource(DriverManager.getConnection(jdbcUrl))
}

object QueryTool {
    def main(args: Array[String]) {
        def usage() {
            Console.err.println("usage: QueryTool jdbc-url query")
        }
        
        args match {
            case Seq(jdbcUrl, q) =>
                for (c <- JdbcUtils.connection(jdbcUrl)) {
                    val rs = c.createStatement().executeQuery(q)
                    val md = rs.getMetaData
                    val cc = md.getColumnCount
                    while (rs.next) {
                        val text = new StringBuilder
                        for (i <- 1 to cc) {
                            val columnName = md.getColumnName(i)
                            val value = rs.getObject(i)
                            if (i > 1) text append " "
                            text append columnName
                            text append "="
                            text append (value match {
                                case null => "NULL"
                                case s: String => "'" + s + "'"
                                case x => x.toString
                            })
                        }
                        println(text)
                    }
                }
            case _ =>
                usage()
                exit(1)
        }
    }
}

// vim: set ts=4 sw=4 et: