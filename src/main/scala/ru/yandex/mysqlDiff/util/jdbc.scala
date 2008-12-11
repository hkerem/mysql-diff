package ru.yandex.mysqlDiff.util

import java.sql._
import javax.sql.DataSource
import scala.collection.mutable.ArrayBuffer

trait LiteDataSource {
    def openConnection(): Connection
    def closeConnection(c: Connection) = c.close()
    /** Close data source */
    def close() = ()
}

object LiteDataSource extends Logging {
    def apply(f: () => Connection): LiteDataSource = new LiteDataSource {
        override def openConnection() = f()
    }
    
    def apply(ds: DataSource): LiteDataSource = apply(() => ds.getConnection())
    
    def driverManager(url: String, user: String, password: String): LiteDataSource = {
        require(url != null && url.length > 0, "url must be not empty")
        apply(() => DriverManager.getConnection(url, user, password))
    }
    
    def driverManager(url: String): LiteDataSource = {
        require(url != null && url.length > 0, "url must be not empty")
        apply(() => DriverManager.getConnection(url))
    }
    
    def singleConnection(c: Connection) = new SingleConnectionLiteDataSource(c)
}

class SingleConnectionLiteDataSource(c: Connection) extends LiteDataSource {
    override def openConnection() = c
    override def closeConnection(c: Connection) = ()
}

class CacheConnectionLiteDataSource(ds: LiteDataSource) extends LiteDataSource {
    private var cached: Option[Connection] = None
    
    override def openConnection() = synchronized {
        cached match {
            case Some(c) => c
            case None =>
                val c = ds.openConnection()
                cached = Some(c)
                c
        }
    }
    
    override def closeConnection(c: Connection) = ()
    
    override def close() = synchronized {
        cached match {
            case Some(c) =>
                ds.closeConnection(c)
                cached = None
            case None =>
        }
    }
}

trait JdbcOperations extends Logging {
    val ds: LiteDataSource
    import ds._
    
    // copy from scalax.resource
    def foreach(f: Connection => Unit): Unit = acquireFor(f)
    def flatMap[B](f: Connection => B): B = acquireFor(f)
    def map[B](f: Connection => B): B = acquireFor(f)

    /** Acquires the resource for the duration of the supplied function. */
    private def acquireFor[B](f: Connection => B): B = {
        val c = openConnection()
        try {
            f(c)
        } finally {
            closeConnectionQuietly(c)
        }
    }
    
    trait Query {
        def prepareStatement(conn: Connection): PreparedStatement
        
        def execute[T](rse: ResultSet => T) =
            JdbcOperations.this.execute { conn =>
                val ps = prepareStatement(conn)
                try {
                    val rs = ps.executeQuery()
                    rse(rs)
                } finally {
                    close(ps)
                }
            }
        
        private def read[T](rs: ResultSet, rm: ResultSet => T): Seq[T] = {
            val r = new ArrayBuffer[T]
            while (rs.next()) {
                r += rm(rs)
            }
            r
        }
        
        def seq[T](rm: ResultSet => T): Seq[T] =
            execute { rs => read(rs, rm) }
        
        private def singleColumnSeq[T](rm: ResultSet => T): Seq[T] = execute { rs =>
            if (rs.getMetaData.getColumnCount != 1) throw new Exception("expecting single column") // XXX
            read(rs, rm)
        }
        
        def ints(): Seq[Int] = singleColumnSeq { rs => rs.getInt(1) }
        
        private class SqlResultSeq[T](s: Seq[T]) {
            def singleRow: T = {
                if (s.length != 1) throw new Exception("expecting 1 row")
                s.first
            }
        }
        
        implicit private def sqlResultSeq[T](s: Seq[T]) = new SqlResultSeq[T](s)
        
        def single[T](rm: ResultSet => T): T = seq(rm).singleRow
        
        def int() = ints().singleRow
    }
    
    /** Close quietly */
    private def close(o: { def close() }) {
        if (o != null)
            try {
                o.close()
            } catch {
                case e => logger.warn("failed to close something: " + e, e)
            }
    }
    
    def closeConnectionQuietly(c: Connection) =
        try {
            closeConnection(c)
        } catch {
            case e => logger.warn("failed to close connection: " + e, e)
        }
    
    def execute[T](ccb: Connection => T): T = {
        val conn = openConnection()
        try {
            ccb(conn)
        } finally {
            closeConnectionQuietly(conn)
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

class JdbcTemplate(override val ds: LiteDataSource) extends JdbcOperations {
    
    def this(ds: () => Connection) = this(LiteDataSource(ds))
    def this(ds: DataSource) = this(LiteDataSource(ds))

}

// vim: set ts=4 sw=4 et:
