package ru.yandex.mysqlDiff.util

import java.sql._
import scala.collection.mutable.ArrayBuffer
import scalax.control.ManagedResource
import scalax.control.UntranslatedManagedResource

class JdbcTemplate(dataSource: () => Connection) extends Logging {

    def openConnection() = dataSource()

    trait Query {
        def prepareStatement(conn: Connection): PreparedStatement
        
        def execute[T](rse: ResultSet => T) =
            JdbcTemplate.this.execute { conn =>
                val ps = prepareStatement(conn)
                try {
                    val rs = ps.executeQuery()
                    rse(rs)
                } finally {
                    close(ps)
                }
            }
        
        def seq[T](rm: ResultSet => T): Seq[T] = execute { rs =>
            val r = new ArrayBuffer[T]
            while (rs.next()) {
                r += rm(rs)
            }
            r
        }
        
    }
    
    private def close(o: { def close() }) {
        if (o != null)
            try {
                o.close()
            } catch {
                case e => logger.warn("failed to close something: " + e, e)
            }
    }
    
    def execute[T](ccb: Connection => T): T = {
        val conn = openConnection()
        try {
            ccb(conn)
        } finally {
            close(conn)
        }
    }
    
    def execute[T](psc: Connection => PreparedStatement, f: PreparedStatement => T): T = {
        execute { conn =>
            val ps = psc(conn)
            try {
                f(ps)
            } finally {
                close(ps)
            }
        }
    }
    
    def execute(q: String) {
        execute { conn =>
            conn.createStatement().execute(q)
        }
    }
    
    class ParamsQuery(q: String, params: Any*) extends Query {
        override def prepareStatement(conn: Connection) = {
            val ps = conn.prepareStatement(q)
            for ((value, i) <- params.toList.zipWithIndex) {
                val c = i + 1
                value match {
                    case s: String => ps.setString(c, s)
                    // XXX: add more
                    case x => ps.setObject(c, x)
                }
            }
            ps
        }
    }
    
    def query(q: String, params: Any*) = new ParamsQuery(q, params: _*)
}

// vim: set ts=4 sw=4 et:
