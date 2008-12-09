package ru.yandex.mysqlDiff.vendor.mysql

object MysqlContext extends Context(MysqlDataTypes) {
    override val sqlParserCombinator = new MysqlParserCombinator(this)
}

case class MysqlServerVersion(major: Int, minor: Int, third: Int) extends Ordered[MysqlServerVersion] {
    override def compare(that: MysqlServerVersion) = {
        if (this.major != that.major) this.major - that.major
        else if (this.minor != that.minor) this.minor - that.minor
        else this.third - that.third
    }
    
    override def toString = major + "." + minor + "." + third
}

object MysqlServerVersion {
    private val VERSION_REGEX = """(\d+)\.(\d+)\.(\d+).*""".r
    
    def parse(versionString: String) =
        versionString match {
            case VERSION_REGEX(major, minor, third) => MysqlServerVersion(major.toInt, minor.toInt, third.toInt)
        }
}

object MysqlServerVersionTests extends org.specs.Specification {
    "compare" in {
        MysqlServerVersion(6, 0, 0) > MysqlServerVersion(5, 2, 0) must beTrue
        MysqlServerVersion(5, 2, 1) > MysqlServerVersion(5, 2, 0) must beTrue
        MysqlServerVersion(5, 2, 1) compare MysqlServerVersion(5, 2, 1) must_== 0
    }
    
    "parse" in {
        MysqlServerVersion.parse("5.0.51a-3-log") must_== MysqlServerVersion(5, 0, 51)
    }
}

class MysqlTests(includeOnline: Boolean) extends org.specs.Specification {
    include(MysqlServerVersionTests)
    if (includeOnline) include(MysqlOnlineTests)
    include(MysqlDataTypesTests)
    include(MysqlParserCombinatorTests)
}

object MysqlTests extends MysqlTests(true)

// vim: set ts=4 sw=4 et:
