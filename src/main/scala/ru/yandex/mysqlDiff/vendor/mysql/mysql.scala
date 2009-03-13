package ru.yandex.mysqlDiff.vendor.mysql

import util._

object MysqlContext extends Context(MysqlDataTypes) {
    override val sqlParserCombinator = new MysqlParserCombinator(this)
    override val dataTypes = MysqlDataTypes
    override val modelParser = new MysqlModelParser(this)
    override val modelSerializer = new MysqlModelSerializer(this)
    override val scriptSerializer = new MysqlScriptSerializer(this)
    override def connectedContext(ds: LiteDataSource) = new MysqlConnectedContext(ds)
}

class MysqlConnectedContext(override val ds: LiteDataSource)
    extends ConnectedContext(MysqlContext, ds)
{
    override val jdbcModelExtractor = new MysqlJdbcModelExtractor(this)
    override val metaDao = new MysqlMetaDao(jt)
}

object MysqlCharsets {
    // SHOW COLLATION saved here
    // this table may not reflect real collations used by system
    private val collations = Seq(
        ("big5_chinese_ci",      "big5",     true),
        ("big5_bin",             "big5",     false),
        ("dec8_swedish_ci",      "dec8",     true),
        ("dec8_bin",             "dec8",     false),
        ("cp850_general_ci",     "cp850",    true),
        ("cp850_bin",            "cp850",    false),
        ("hp8_english_ci",       "hp8",      true),
        ("hp8_bin",              "hp8",      false),
        ("koi8r_general_ci",     "koi8r",    true),
        ("koi8r_bin",            "koi8r",    false),
        ("latin1_german1_ci",    "latin1",   false),
        ("latin1_swedish_ci",    "latin1",   true),
        ("latin1_danish_ci",     "latin1",   false),
        ("latin1_german2_ci",    "latin1",   false),
        ("latin1_bin",           "latin1",   false),
        ("latin1_general_ci",    "latin1",   false),
        ("latin1_general_cs",    "latin1",   false),
        ("latin1_spanish_ci",    "latin1",   false),
        ("latin2_czech_cs",      "latin2",   false),
        ("latin2_general_ci",    "latin2",   true),
        ("latin2_hungarian_ci",  "latin2",   false),
        ("latin2_croatian_ci",   "latin2",   false),
        ("latin2_bin",           "latin2",   false),
        ("swe7_swedish_ci",      "swe7",     true),
        ("swe7_bin",             "swe7",     false),
        ("ascii_general_ci",     "ascii",    true),
        ("ascii_bin",            "ascii",    false),
        ("ujis_japanese_ci",     "ujis",     true),
        ("ujis_bin",             "ujis",     false),
        ("sjis_japanese_ci",     "sjis",     true),
        ("sjis_bin",             "sjis",     false),
        ("hebrew_general_ci",    "hebrew",   true),
        ("hebrew_bin",           "hebrew",   false),
        ("tis620_thai_ci",       "tis620",   true),
        ("tis620_bin",           "tis620",   false),
        ("euckr_korean_ci",      "euckr",    true),
        ("euckr_bin",            "euckr",    false),
        ("koi8u_general_ci",     "koi8u",    true),
        ("koi8u_bin",            "koi8u",    false),
        ("gb2312_chinese_ci",    "gb2312",   true),
        ("gb2312_bin",           "gb2312",   false),
        ("greek_general_ci",     "greek",    true),
        ("greek_bin",            "greek",    false),
        ("cp1250_general_ci",    "cp1250",   true),
        ("cp1250_czech_cs",      "cp1250",   false),
        ("cp1250_croatian_ci",   "cp1250",   false),
        ("cp1250_bin",           "cp1250",   false),
        ("gbk_chinese_ci",       "gbk",      true),
        ("gbk_bin",              "gbk",      false),
        ("latin5_turkish_ci",    "latin5",   true),
        ("latin5_bin",           "latin5",   false),
        ("armscii8_general_ci",  "armscii8", true),
        ("armscii8_bin",         "armscii8", false),
        ("utf8_general_ci",      "utf8",     true),
        ("utf8_bin",             "utf8",     false),
        ("utf8_unicode_ci",      "utf8",     false),
        ("utf8_icelandic_ci",    "utf8",     false),
        ("utf8_latvian_ci",      "utf8",     false),
        ("utf8_romanian_ci",     "utf8",     false),
        ("utf8_slovenian_ci",    "utf8",     false),
        ("utf8_polish_ci",       "utf8",     false),
        ("utf8_estonian_ci",     "utf8",     false),
        ("utf8_spanish_ci",      "utf8",     false),
        ("utf8_swedish_ci",      "utf8",     false),
        ("utf8_turkish_ci",      "utf8",     false),
        ("utf8_czech_ci",        "utf8",     false),
        ("utf8_danish_ci",       "utf8",     false),
        ("utf8_lithuanian_ci",   "utf8",     false),
        ("utf8_slovak_ci",       "utf8",     false),
        ("utf8_spanish2_ci",     "utf8",     false),
        ("utf8_roman_ci",        "utf8",     false),
        ("utf8_persian_ci",      "utf8",     false),
        ("utf8_esperanto_ci",    "utf8",     false),
        ("utf8_hungarian_ci",    "utf8",     false),
        ("ucs2_general_ci",      "ucs2",     true),
        ("ucs2_bin",             "ucs2",     false),
        ("ucs2_unicode_ci",      "ucs2",     false),
        ("ucs2_icelandic_ci",    "ucs2",     false),
        ("ucs2_latvian_ci",      "ucs2",     false),
        ("ucs2_romanian_ci",     "ucs2",     false),
        ("ucs2_slovenian_ci",    "ucs2",     false),
        ("ucs2_polish_ci",       "ucs2",     false),
        ("ucs2_estonian_ci",     "ucs2",     false),
        ("ucs2_spanish_ci",      "ucs2",     false),
        ("ucs2_swedish_ci",      "ucs2",     false),
        ("ucs2_turkish_ci",      "ucs2",     false),
        ("ucs2_czech_ci",        "ucs2",     false),
        ("ucs2_danish_ci",       "ucs2",     false),
        ("ucs2_lithuanian_ci",   "ucs2",     false),
        ("ucs2_slovak_ci",       "ucs2",     false),
        ("ucs2_spanish2_ci",     "ucs2",     false),
        ("ucs2_roman_ci",        "ucs2",     false),
        ("ucs2_persian_ci",      "ucs2",     false),
        ("ucs2_esperanto_ci",    "ucs2",     false),
        ("ucs2_hungarian_ci",    "ucs2",     false),
        ("cp866_general_ci",     "cp866",    true),
        ("cp866_bin",            "cp866",    false),
        ("keybcs2_general_ci",   "keybcs2",  true),
        ("keybcs2_bin",          "keybcs2",  false),
        ("macce_general_ci",     "macce",    true),
        ("macce_bin",            "macce",    false),
        ("macroman_general_ci",  "macroman", true),
        ("macroman_bin",         "macroman", false),
        ("cp852_general_ci",     "cp852",    true),
        ("cp852_bin",            "cp852",    false),
        ("latin7_estonian_cs",   "latin7",   false),
        ("latin7_general_ci",    "latin7",   true),
        ("latin7_general_cs",    "latin7",   false),
        ("latin7_bin",           "latin7",   false),
        ("cp1251_bulgarian_ci",  "cp1251",   false),
        ("cp1251_ukrainian_ci",  "cp1251",   false),
        ("cp1251_bin",           "cp1251",   false),
        ("cp1251_general_ci",    "cp1251",   true),
        ("cp1251_general_cs",    "cp1251",   false),
        ("cp1256_general_ci",    "cp1256",   true),
        ("cp1256_bin",           "cp1256",   false),
        ("cp1257_lithuanian_ci", "cp1257",   false),
        ("cp1257_bin",           "cp1257",   false),
        ("cp1257_general_ci",    "cp1257",   true),
        ("binary",               "binary",   true),
        ("geostd8_general_ci",   "geostd8",  true),
        ("geostd8_bin",          "geostd8",  false),
        ("cp932_japanese_ci",    "cp932",    true),
        ("cp932_bin",            "cp932",    false),
        ("eucjpms_japanese_ci",  "eucjpms",  true),
        ("eucjpms_bin",          "eucjpms",  false) // trailing comma is deprecated, unfortunately
    )

    private val defaultCollations = scala.collection.immutable.Map(
        collations.flatMap {
            case (a, b, true)  => Some((b, a))
            case (_, _, false) => None
        }: _*
    )
    
    private val defaultCharsets = scala.collection.immutable.Map(
        collations.map { case (a, b, _) => (a, b) } : _*
    )
    
    /** Default collection for charset */
    def defaultCollation(charset: String) = defaultCollations.get(charset.toLowerCase)
    
    /** Charset for collation */
    def defaultCharset(collation: String) = defaultCharsets.get(collation.toLowerCase)

}

object MysqlCharsetsTests extends org.specs.Specification {
    import MysqlCharsets._
    
    "defaultCollation" in {
        defaultCollation("utf8") must_== Some("utf8_general_ci")
        defaultCollation("fgf") must_== None
    }
    
    "defaultCharset" in {
        defaultCharset("cp1256_general_ci") must_== Some("cp1256")
        defaultCharset("fgf") must_== None
    }
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
    include(MysqlCharsetsTests)
    include(MysqlModelParserTests)
    if (includeOnline) include(MysqlOnlineTests)
    include(MysqlDataTypesTests)
    include(MysqlParserCombinatorTests)
    if (includeOnline) include(MysqlJdbcModelExtractorTests)
    include(MysqlDataTypesTests)
}

object MysqlTests extends MysqlTests(true)

// vim: set ts=4 sw=4 et:
