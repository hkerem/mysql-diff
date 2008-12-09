package ru.yandex.mysqlDiff.vendor

import util._

trait TestDataSourceParameters {
    val testDsUrl: String
    val testDsUser: String
    val testDsPassword: String
    
    val ds = LiteDataSource.driverManager(testDsUrl, testDsUser, testDsPassword)
    
    val jdbcTemplate = new util.JdbcTemplate(ds)
}

// vim: set ts=4 sw=4 et:
