package ru.yandex.mysqlDiff.vendor.postgresql

import model._

object PostgresqlDataTypes extends DataTypes {
    override def int = make("INTEGER")
    
    override def make(name: String, length: Option[Int], options: Seq[DataTypeOption]) =
        new PostgresqlDataType(name, length, options)
}

class PostgresqlDataType(override val name: String, override val length: Option[int], override val options: Seq[DataTypeOption])
    extends DataType(name, length, options) {

    def isAnyChar = name.matches(".*CHAR")
    def isAnyDateTime = List("DATE", "TIME", "TIMESTAMP") contains name
    def isAnyNumber = name.matches("(SMALL|BIG)INT|INTEGER") ||
        (List("NUMBER", "REAL", "DOUBLE PRECISION", "DECIMAL", "NUMERIC") contains name)

    def isLengthAllowed = !(isAnyDateTime || name.matches("TEXT|BYTEA"))

    def normalized = { // FIXME
        new PostgresqlDataType(name, length, options)
    }
}

// vim: set ts=4 sw=4 et:
