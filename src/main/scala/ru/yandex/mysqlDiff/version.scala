package ru.yandex.mysqlDiff

class MysqlDiffVersion(version: String, buildDate: String) {
    override def toString = version + " (" + buildDate + ")"
}

object MysqlDiffVersion {
    val version = try {
        val ps = new java.util.Properties
        val is = classOf[MysqlDiffVersion].getClassLoader.getResourceAsStream("mysql-diff.properties")
        try {
            ps.load(is)
        } finally {
            is.close()
        }
        val v = ps.getProperty("project.version") match {
            case null | "" => "unknown"
            case x => x
        }
        val d = ps.getProperty("build.date") match {
            case null | "" => "some day"
            case x => x
        }
        new MysqlDiffVersion(v, d)
    } catch {
        case e => new MysqlDiffVersion("failed to get mysql-diff.properties: " + e, "some day")
    }
    
}

// vim: set ts=4 sw=4 et:
