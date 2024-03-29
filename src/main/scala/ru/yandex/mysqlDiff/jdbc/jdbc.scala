package ru.yandex.mysqlDiff
package jdbc

import java.sql.DriverManager

import ru.yandex.small.jdbc._

import Implicits._

/**
 * Commandline query tool.
 */
object QueryTool {
    def main(args0: Array[String]) {
        def usage() {
            Console.err.println("usage: QueryTool jdbc-url query")
        }

        val args: Seq[String] = args0.toSeq

        args match {
            case Seq(jdbcUrl, q) =>
                for (c <- LiteDataSource.driverManager(jdbcUrl)) {
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

class JdbcTests(testsSelector: TestsSelector) extends MySpecification {
    include(new MetaDaoTests(testsSelector))
}

object JdbcTests extends JdbcTests(AllTestsSelector)

// vim: set ts=4 sw=4 et:
