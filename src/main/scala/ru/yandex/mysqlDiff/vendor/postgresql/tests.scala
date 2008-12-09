package ru.yandex.mysqlDiff.vendor.postgresql

import java.sql._

import jdbc._
import diff._
import model._
import script._
import util._

object PostgresqlTestDataSourceParameters extends TestDataSourceParameters {
    //Class.forName("")
    
    override val testDsUrl = "jdbc:postgresql:mysql_diff_tests"
    override val testDsUser = "test"
    override val testDsPassword = "test"
    
}

// vim: set ts=4 sw=4 et:
