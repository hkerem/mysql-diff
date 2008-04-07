package ru.yandex.mysqlDiff.model

case class DataType(val name: String, val length: Option[Int]) {
    
}

case class Column(val name: String, val dataType: DataType) {
    
}

abstract class DatabaseDeclaration

case class Table(val name: String, val columns: Seq[Column]) extends DatabaseDeclaration {
    
}

case class Database(val declarations: Seq[DatabaseDeclaration])