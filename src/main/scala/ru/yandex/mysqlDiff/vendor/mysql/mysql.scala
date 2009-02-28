package ru.yandex.mysqlDiff.vendor.mysql

import scala.collection.immutable.Set

object MysqlContext extends Context(MysqlDataTypes) {
    override val sqlParserCombinator = new MysqlParserCombinator(this)
    override val dataTypes = MysqlDataTypes
    override val modelParser = new MysqlModelParser(this)
}

object MysqlCollation {
    // copied from SHOW COLLATION
    private val defaultCollations = scala.collection.immutable.Map(
        "armscii8" -> "armscii8_general_ci",
        "ascii"    -> "ascii_general_ci",
        "big5"     -> "big5_chinese_ci",
        "binary"   -> "binary",
        "cp1250"   -> "cp1250_general_ci",
        "cp1251"   -> "cp1251_general_ci",
        "cp1256"   -> "cp1256_general_ci",
        "cp1257"   -> "cp1257_general_ci",
        "cp850"    -> "cp850_general_ci",
        "cp852"    -> "cp852_general_ci",
        "cp866"    -> "cp866_general_ci",
        "cp932"    -> "cp932_japanese_ci",
        "dec8"     -> "dec8_swedish_ci",
        "eucjpms"  -> "eucjpms_japanese_ci",
        "euckr"    -> "euckr_korean_ci",
        "gb2312"   -> "gb2312_chinese_ci",
        "gbk"      -> "gbk_chinese_ci",
        "geostd8"  -> "geostd8_general_ci",
        "greek"    -> "greek_general_ci",
        "hebrew"   -> "hebrew_general_ci",
        "hp8"      -> "hp8_english_ci",
        "keybcs2"  -> "keybcs2_general_ci",
        "koi8r"    -> "koi8r_general_ci",
        "koi8u"    -> "koi8u_general_ci",
        "latin1"   -> "latin1_swedish_ci",
        "latin2"   -> "latin2_general_ci",
        "latin5"   -> "latin5_turkish_ci",
        "latin7"   -> "latin7_general_ci",
        "macce"    -> "macce_general_ci",
        "macroman" -> "macroman_general_ci",
        "sjis"     -> "sjis_japanese_ci",
        "swe7"     -> "swe7_swedish_ci",
        "tis620"   -> "tis620_thai_ci",
        "ucs2"     -> "ucs2_general_ci",
        "ujis"     -> "ujis_japanese_ci",
        "utf8"     -> "utf8_general_ci"
    )
    
    def defaultCollation(charset: String) = defaultCollations.get(charset.toLowerCase)

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
