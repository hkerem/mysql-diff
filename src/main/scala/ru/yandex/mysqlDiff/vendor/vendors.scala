package ru.yandex.mysqlDiff.vendor

class VendorTests(includeOnline: Boolean) extends org.specs.Specification {
    include(new mysql.MysqlTests(includeOnline))
}

object VendorTests extends VendorTests(true)

// vim: set ts=4 sw=4 et:
