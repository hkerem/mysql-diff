package ru.yandex.mysqlDiff.vendor

import util._

trait TestDataSourceParameters {
    val defaultTestDsUrl: String
    val dbName =
        this.getClass.getName
            .replaceFirst("^ru.yandex.mysqlDiff.vendor.", "")
            .replaceFirst("\\..*", "")
            .ensuring { _.matches("\\w+") }
    val testDsUrlProperty = dbName + ".test.ds.url"
    
    val testDsUrl = System.getProperty(testDsUrlProperty) match {
        case null | "" => defaultTestDsUrl
        case x => x
    }
    val testDsUser: String
    val testDsPassword: String
    
    val ds = LiteDataSource.driverManager(testDsUrl, testDsUser, testDsPassword)
    
    val jdbcTemplate = new util.JdbcTemplate(ds)
}

// vim: set ts=4 sw=4 et:
