package ru.yandex.mysqlDiff.vendor.mysql

import model._

case object MysqlZerofill extends DataTypeOption
case object MysqlUnsigned extends DataTypeOption
case class MysqlCharacterSet(name: String) extends DataTypeOption
case class MysqlCollate(name: String) extends DataTypeOption

case class MysqlDataType(override val name: String, override val length: Option[int], override val options: Seq[DataTypeOption])
    extends DataType(name, length, options) {

    def isAnyChar = name.matches(".*CHAR")
    def isAnyDateTime = List("DATE", "TIME", "DATETIME", "TIMESTAMP") contains name
    def isAnyNumber = name.matches("(|TINY|SMALL|BIG)INT") ||
        (List("NUMBER", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC") contains name)
    def isLengthAllowed = !(isAnyDateTime || name.matches("(TINY|MEDIUM|LONG|)(TEXT|BLOB)"))
    
    override def normalized = MysqlDataTypes.normalize(this)
}

object MysqlDataTypes extends DataTypes {
    def int = make("INT")

    override def make(name: String, length: Option[Int], options: Seq[DataTypeOption]): DataType =
        new MysqlDataType(name, length, options)

    override def normalize(dt: DataType) = super.normalize(dt) match {
        case MysqlDataType("BIT", _, options) => new MysqlDataType("TINYINT", Some(1), options)
        case dt => dt
    }
}

object MysqlDataTypesTests extends org.specs.Specification {
    import MysqlContext._

    "TINYINT(1) equivalent to BIT" in {
        dataTypes.equivalent(dataTypes.make("BIT"), dataTypes.make("TINYINT", Some(1))) must beTrue
    }
}

// vim: set ts=4 sw=4 et:
