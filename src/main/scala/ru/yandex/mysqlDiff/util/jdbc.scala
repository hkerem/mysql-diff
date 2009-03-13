package ru.yandex.mysqlDiff.util

import java.sql._
import javax.sql.DataSource
import scala.collection.mutable.ArrayBuffer

trait JdbcImplicits {
    implicit def jdbcTemplate(ds: LiteDataSource) = new JdbcTemplate(ds)
    
    implicit def resultSetExtras(rs: ResultSet) = new ResultSetExtras(rs)
}

object JdbcImplicits extends JdbcImplicits

import JdbcImplicits._

/**
 * Slightly different variant of <code>javax.sql.DataSource</code>
 * 
 * @see http://jira.springframework.org/browse/SPR-5532
 */
trait LiteDataSource {
    /** Obtain new connection */
    def openConnection(): Connection
    /**
      * Release connection obtained by this data source,
      * just delegate to <code>Connection.close()</code> in this impl
      */
    def closeConnection(c: Connection) = c.close()
    /** Close data source, do nothing by default */
    def close() = ()
}

/**
 * Factories to create data source
 */
object LiteDataSource extends Logging {
    /** Create from function, returning connection */
    def apply(f: () => Connection): LiteDataSource = new LiteDataSource {
        override def openConnection() = f()
    }
    
    /** Wrap <code>javax.sql.DataSource</code> */
    def apply(ds: DataSource): LiteDataSource = apply(() => ds.getConnection())
    
    /** Obtain from <code>DriverManager</code> using specified URL, login and password */
    def driverManager(url: String, user: String, password: String): LiteDataSource = {
        require(url != null && url.length > 0, "url must be not empty")
        apply(() => DriverManager.getConnection(url, user, password))
    }
    
    /** Obtain from <code>DriverManager</code> by URL */
    def driverManager(url: String): LiteDataSource = {
        require(url != null && url.length > 0, "url must be not empty")
        apply(() => DriverManager.getConnection(url))
    }
    
    /** Data source that always feeds same connection. Connection is never closed by data source */
    def singleConnection(c: Connection) = new SingleConnectionLiteDataSource(c)
    
}

/**
 * Simple data source that shares same connection
 */
class SingleConnectionLiteDataSource(c: Connection) extends LiteDataSource {
    override def openConnection() = c
    override def closeConnection(c: Connection) = ()
}

/**
 * Data source that obtains connection from the given data souce and caches it.
 */
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
    
    /** Release cached connection */
    override def close() = synchronized {
        cached match {
            case Some(c) =>
                ds.closeConnection(c)
                cached = None
            case None =>
        }
    }
}

/** JdbcTemplate implementation */
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
        
        def seq[T](rm: ResultSet => T): Seq[T] =
            execute { rs => rs.read(rm) }
        
        private def singleColumnSeq[T](rm: ResultSet => T): Seq[T] = execute { rs =>
            if (rs.getMetaData.getColumnCount != 1) throw new Exception("expecting single column") // XXX
            rs.read(rm)
        }
        
        def ints(): Seq[Int] = singleColumnSeq { rs => rs.getInt(1) }
        
        /** Internal helper utility */
        private class SqlResultSeq[T](s: Seq[T]) {
            /** @return first element, or throws */
            def singleRow: T = {
                if (s.length == 1) s.first
                else throw new Exception("expecting 1 row, got " + s.length)
            }
            
            /** @return None, or Some(first), or throws if more then one element */
            def optionRow: Option[T] = {
                if (s.length == 0) None
                else if (s.length == 1) Some(s.first)
                else throw new Exception("expecting 0 or 1 row, got " + s.length)
            }
        }
        
        implicit private def sqlResultSeq[T](s: Seq[T]) = new SqlResultSeq[T](s)
        
        def single[T](rm: ResultSet => T): T = seq(rm).singleRow
        
        def option[T](rm: ResultSet => T): Option[T] = seq(rm).optionRow
        
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
    
    /** Specify SQL and params for query, result object can be used to actually query the data */
    def query(q: String, params: Any*) = new ParamsQuery(q, params: _*)
    
    /** Fetch meta data in the safe way */
    def metaData[T](cb: DatabaseMetaData => T) = acquireFor { c: Connection => cb(c.getMetaData) }
}

/** Canonical JdbcTemplate */
class JdbcTemplate(override val ds: LiteDataSource) extends JdbcOperations {
    
    def this(ds: () => Connection) = this(LiteDataSource(ds))
    def this(ds: DataSource) = this(LiteDataSource(ds))

}

class ResultSetExtras(rs: ResultSet) {
    import rs._
    
    private def mapNull[T <: AnyRef](value: T) = value match {
        case null => None
        case value => Some(value)
    }
    private def mapWasNull[T](value: T) =
        if (wasNull) None
        else Some(value)
    
    def getStringOption(column: Int) = mapNull(rs.getString(column))
    def getStringOption(column: String) = mapNull(rs.getString(column))
    
    def getIntOption(column: Int) = mapWasNull(rs.getInt(column))
    def getIntOption(column: String) = mapWasNull(rs.getInt(column))
    
    def read[T](rm: ResultSet => T): Seq[T] = {
        val r = new ArrayBuffer[T]
        while (rs.next()) {
            r += rm(rs)
        }
        r
    }
    
    def readValues() =
        read {
            rs =>
                (1 to rs.getMetaData.getColumnCount)
                    .map(i => (rs.getMetaData.getColumnName(i), rs.getObject(i)))
                    .toList
        }
}

// vim: set ts=4 sw=4 et:
