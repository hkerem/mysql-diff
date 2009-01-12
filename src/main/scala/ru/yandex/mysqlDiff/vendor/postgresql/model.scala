package ru.yandex.mysqlDiff.vendor.postgresql

import model._

object PostgresqlDataTypes extends DataTypes {
    override def int = make("INTEGER")
    
    override def make(name: String, length: Option[Int], options: Seq[DataTypeOption]) =
        new PostgresqlDataType(name, length, options)
    
    override def resolveTypeNameAlias(name: String) = name match {
        case "INT8" => "BIGINT"
        case "SERIAL8" => "BIGSERIAL"
        case "VARBIT" => "BIT VARYING"
        case "BOOL" => "BOOLEAN"
        case "VARCHAR" => "CHARACTER VARYING"
        case "CHAR" => "CHARACTER"
        case "FLOAT8" => "DOUBLE PRECISION"
        case "INT" | "INT4" => "INTEGER"
        case "DECIMAL" => "NUMERIC"
        case "FLOAT4" => "REAL"
        case "INT2" => "SMALLINT"
        case "SERIAL4" => "SERIAL"
        // XXX: TIME*TZ
        case x => x
    }
}

case class PostgresqlDataType(override val name: String, override val length: Option[int], override val options: Seq[DataTypeOption])
    extends DataType(name, length, options) {

    def isAnyChar = name.matches(".*CHAR")
    def isAnyDateTime = List("DATE", "TIME", "TIMESTAMP") contains name
    def isAnyNumber = name.matches("(SMALL|BIG)INT|INTEGER") ||
        (List("NUMBER", "REAL", "DOUBLE PRECISION", "DECIMAL", "NUMERIC") contains name)

    def isLengthAllowed = !(isAnyDateTime || name.matches("TEXT|BYTEA"))

    def normalized = PostgresqlDataTypes.normalize(this)
}

// vim: set ts=4 sw=4 et:
