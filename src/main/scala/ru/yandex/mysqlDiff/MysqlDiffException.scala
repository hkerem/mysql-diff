package ru.yandex.mysqlDiff

class MysqlDiffException(val message: String) extends Exception(message)

class UnsupportedFeatureException(override val message: String) extends MysqlDiffException(message)

// vim: set ts=4 sw=4 et:
