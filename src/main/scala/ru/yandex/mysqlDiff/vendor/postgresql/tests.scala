package ru.yandex.mysqlDiff.vendor.postgresql

import java.sql._

import jdbc._
import diff._
import model._
import script._
import util._

object PostgresqlTestDataSourceParameters extends TestDataSourceParameters {
    //Class.forName("")
    
    /// to create database
    // CREATE DATABASE mysql_diff_tests
    // CREATE USER test
    // ALTER ROLE test WITH PASSWORD 'test'
    // GRANT ALL ON DATABASE mysql_diff_tests TO test
    
    /// connect to database from console
    // PGPASSWORD=test psql --host=localhost --user test mysql_diff_tests
    
    override val testDsUrl = "jdbc:postgresql:mysql_diff_tests"
    override val testDsUser = "test"
    override val testDsPassword = "test"
    
}

// vim: set ts=4 sw=4 et:
