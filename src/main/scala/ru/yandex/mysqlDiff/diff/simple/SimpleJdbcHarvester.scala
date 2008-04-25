package ru.yandex.mysqlDiff.diff.simple;

import java.sql._

import ru.yandex.mysqlDiff.model._


object SimpleJdbcHarvester {
  
  
  def parseTable(tables: ResultSet, data: DatabaseMetaData): TableModel = {
    val tableName = tables.getString("TABLE_NAME");
    val columns = data.getColumns(null, null, tableName, "%");

    System.out.println("Table: " + tableName);
    while (columns.next) {
        val colName = columns.getString("COLUMN_NAME");
        val colType = columns.getString("TYPE_NAME");
        var colTypeSize: int = -1;
        if (columns.getObject("COLUMN_SIZE") != null) colTypeSize = columns.getInt("COLUMN_SIZE");
        
        val isNotNull = !columns.getString("IS_NULLABLE").equalsIgnoreCase("yes");
        val isAutoinrement = columns.getString("IS_AUTOINCREMENT").equalsIgnoreCase("YES");

        System.out.println("Column: " + colName + ", type: " + colType + ", colTypeSize: " + colTypeSize + " IsNotnull: " + isNotNull + ", isautoincrement:" +  isAutoinrement);
    }
    null
  }
  
  
  def parsePrimaryKeys(table: TableModel, data: DatabaseMetaData): PrimaryKeyModel = {
    val primaryKeys: ResultSet = data.getPrimaryKeys(null, null, table.name)
    while (primaryKeys.next) {
      val column = primaryKeys.getString("COLUMN_NAME")
      val indexName = primaryKeys.getString("PK_NAME")
      System.out.println("p name: " + indexName + " c name: " + column)
    }
    return null;
  }
  
  
  def parseIndexes(table: TableModel, data: DatabaseMetaData): Seq[IndexModel] = {
    val indexes = data.getIndexInfo(null, null, table.name, false, true)
    while (indexes.next) {
       val colName = indexes.getString("COLUMN_NAME");
       val indexName = indexes.getString("INDEX_NAME");
       System.out.println("non unique name: " + indexName + " c name: " + colName);
    }
    List()
  }
  
  def search(url: String, user: String, pass: String): Seq[TableModel] = {
    Class.forName("com.mysql.jdbc.Driver").newInstance;

    val conn: Connection = DriverManager.getConnection(url, user, pass);
    val data: DatabaseMetaData = conn.getMetaData;
    
    val tables: ResultSet = data.getTables(null, "%", "%", List("TABLE").toArray);

    
    
    while (tables.next) {
        val tableModel = parseTable(tables, data);    

        Console.println("Primary key:")
        val pk = parsePrimaryKeys(tableModel, data)
        val indexes = parseIndexes(tableModel, data);
    }
    null
  }
  
  
  def parse(connectionString: String, user: String, pass: String): DatabaseModel = {
    new DatabaseModel("database", search(connectionString, user, pass))
  }  
}
