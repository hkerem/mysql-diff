package ru.yandex.mysqlDiff.vendor.postgresql

import model._

object PostgresqlDataTypes extends DataTypes {
    override def int = make("INTEGER")
    
    override def resolveTypeNameAlias(name: String) = name.toUpperCase match {
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

class PostgresqlModelSerializer(context: Context) extends ModelSerializer(context) {
    import context._
    
    override def serializeColumn(column: ColumnModel) =
        // because PostgreSQL DEFAULT NULL could interpret as DEFAULT 'NULL::character varying'
        super.serializeColumn(
            column.withProperties(column.properties.removeProperty(DefaultValue(NullValue))))
}

// vim: set ts=4 sw=4 et:
