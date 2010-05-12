package ru.yandex.mysqlDiff
package util

object SystemInfo {
    def javaVersion =
        System.getProperty("java.version")
    
    private val VERSION_REGEXP =
        new scala.util.matching.Regex("1\\.(\\d).*")
    
    // 5, 6, 7
    def javaVersionMajor =
        javaVersion match {
            case VERSION_REGEXP(d) => d.toInt
        }
}

object SystemInfoTests extends org.specs.Specification {
    "javaVersionMajor" in {
        SystemInfo.javaVersionMajor must beGreaterThanOrEqualTo(5)
        SystemInfo.javaVersionMajor must beLessThanOrEqualTo(7)
    }
}

// vim: set ts=4 sw=4 et:
