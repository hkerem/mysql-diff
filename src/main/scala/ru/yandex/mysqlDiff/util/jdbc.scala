package ru.yandex.mysqlDiff.util

/*
import java.sql._

class JdbcTemplate(dataSource: () => Connection) extends Logging {
    trait Query {
        def prepareStatement(): PreparedStatement
        
        def execute[T](rse: ResultSet => T) = T
        
        def seq[T](rm: ResultSet => T) = 
        
    }
    
    private def close(o: { def close() }) {
        if (o != null)
            try {
                o.close()
            } catch {
                case e => logger.warn("failed to close something")
            }
    }
    
    def execute[T](ccb: Connection => T) = {
        val conn = dataSource()
        try {
            ccb(conn)
        } finally {
            close(conn)
        }
    }
    
    def execute[T](psc: Connection => PreparedStatement, f: PreparedStatement => T) = {
        val ps = psc()
        try {
            f(ps)
        } finally {
            close(ps)
        }
    }
    
    class ParamsQuery(q: String, params: Any*) extends Query {
        override def prepareStatement() = {
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
    
    def query(q: String, params: Any*) = new paramsQuery(q, params: _*)
}
*/

// vim: set ts=4 sw=4 et:
