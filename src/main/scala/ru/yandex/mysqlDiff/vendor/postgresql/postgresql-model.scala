package ru.yandex.mysqlDiff
package vendor.postgresql

import model._

import Implicits._

object PostgresqlDataTypes extends DataTypes {
    override def int = make("INTEGER")
    
    override def resolveTypeNameAlias(name: String) = name.toUpperCase match {
        // XXX: serial should be resoled to sequences when model is parsed
        
        case "INT8" => "BIGINT"
        //case "SERIAL8" => "BIGSERIAL"
        case "SERIAL8" | "BIGSERIAL" => "BIGINT"
        
        case "VARBIT" => "BIT VARYING"
        case "BOOL" => "BOOLEAN"
        case "VARCHAR" => "CHARACTER VARYING"
        case "CHAR" => "CHARACTER"
        case "FLOAT8" => "DOUBLE PRECISION"
        
        case "INT" | "INT4" => "INTEGER"
        //case "SERIAL4" => "SERIAL"
        case "SERIAL" | "SERIAL4" => "INTEGER"
        
        case "DECIMAL" => "NUMERIC"
        case "FLOAT4" => "REAL"
        case "INT2" => "SMALLINT"
        // XXX: TIME*TZ
        case x => x
    }
    
    override def make(name: String, length: Option[Int]): DataType =
        if (name startsWith "_")
            ArrayDataType(super.make(name.replaceFirst("^_", "")))
        else
            super.make(name, length)
    
}

class PostgresqlModelSerializer(context: Context) extends ModelSerializer(context) {
    import context._
    
    override def serializeColumn(column: ColumnModel) =
        // because PostgreSQL DEFAULT NULL could interpret as DEFAULT 'NULL::character varying'
        super.serializeColumn(
            column.withProperties(column.properties.removeProperty(DefaultValue(NullValue))))
}

class PostgresqlModelParser(override val context: Context) extends ModelParser(context) {
    import context._
    
    override def fixColumn(column: ColumnModel, table: TableModel) =
        // XXX: hack
        super.fixColumn(column.withProperties(
            column.properties.removePropertyByType(vendor.mysql.MysqlAutoIncrementPropertyType)), table)
}

// vim: set ts=4 sw=4 et:
