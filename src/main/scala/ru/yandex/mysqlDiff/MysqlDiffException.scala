package ru.yandex.mysqlDiff

class MysqlDiffException(val message: String, cause: Throwable)
    extends Exception(message, cause)
{
    def this(message: String) = this(message, null)
}

class UnsupportedFeatureException(override val message: String) extends MysqlDiffException(message)

// vim: set ts=4 sw=4 et:
