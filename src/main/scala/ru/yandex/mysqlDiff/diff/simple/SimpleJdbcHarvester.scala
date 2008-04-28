package ru.yandex.mysqlDiff.diff.simple;

import java.sql._

import ru.yandex.mysqlDiff.model._


object SimpleJdbcHarvester {
  
  
  def parseTable(tables: ResultSet, data: DatabaseMetaData): TableModel = {
    val tableName = tables.getString("TABLE_NAME");
    val columns = data.getColumns(null, null, tableName, "%");
    var columnsList = List[ColumnModel]()

    while (columns.next) {
        val colName = columns.getString("COLUMN_NAME");
        val colType = columns.getString("TYPE_NAME");
        var colTypeSize: int = -1;
        if (columns.getObject("COLUMN_SIZE") != null) colTypeSize = columns.getInt("COLUMN_SIZE");
        
        val isNotNull = !columns.getString("IS_NULLABLE").equalsIgnoreCase("yes");
        val isAutoinrement = columns.getString("IS_AUTOINCREMENT").equalsIgnoreCase("YES");
        
        
        var typeSizeOption: Option[int] = None;
        if (colTypeSize  != -1) typeSizeOption = Some(colTypeSize)
        
        val cm = new ColumnModel(colName, new DataType(colType, typeSizeOption))
        cm.isNotNull = isNotNull
        cm.isAutoIncrement = isAutoinrement
        
        columnsList = List((columnsList ++ List(cm)): _*)
    }
    
    val tm  = new TableModel(tableName, columnsList)
    columnsList.foreach(x => x.parent = tm)
    tm
  }
  
  
  def parsePrimaryKeys(table: TableModel, data: DatabaseMetaData): PrimaryKeyModel = {
    
    var pkColumns = List[String]()
    
    var pkName = ""
    
    val primaryKeys: ResultSet = data.getPrimaryKeys(null, null, table.name)
    while (primaryKeys.next) {
      val column = primaryKeys.getString("COLUMN_NAME")
      val indexName = primaryKeys.getString("PK_NAME")
      pkColumns = List((pkColumns ++ List(column)): _*)
      pkName = indexName
    }
    if (pkColumns.size > 0) {
      var pk = new PrimaryKeyModel(pkName, pkColumns);
      pk.parent = table;
      table.primaryKey = pk;
      pk
    } else null
  }
  
  
  
  def parseIndexes(table: TableModel, data: DatabaseMetaData, checkPrimaryKey: boolean): Seq[IndexModel] = {
    var indexesMap: Map[String, List[String]] = Map()  
    val indexes = data.getIndexInfo(null, null, table.name, false, true)
    while (indexes.next) {
       val colName = indexes.getString("COLUMN_NAME");
       val indexName = indexes.getString("INDEX_NAME");
       if (!checkPrimaryKey || table.primaryKey == null || table.primaryKey.name == null || !table.primaryKey.name.equals(indexName))
         if (indexesMap.contains(indexName))
           indexesMap(indexName) = (indexesMap(indexName) ++ List(colName)).toList
         else
           indexesMap(indexName) = List(colName)
    }
    
    val resultList = indexesMap.map(x => new IndexModel(x._1, x._2, false)).filter(x => {x.parent = table; true})
    if (table.keys == null) 
      table.keys = resultList.toList
      else
        table.keys = (table.keys ++ resultList.toList).toList
    resultList.toList
  }
  
  
  
  def parseIndexes(table: TableModel, data: DatabaseMetaData): Seq[IndexModel] = {
    parseIndexes(table, data, false)
  }

  
  def parseUnique(table: TableModel, data: DatabaseMetaData): Seq[IndexModel] = {
    parseIndexes(table, data, true)
  }
  
  
  def search(url: String): Seq[TableModel] = {
    Class.forName("com.mysql.jdbc.Driver").newInstance;

    val conn: Connection = DriverManager.getConnection(url);
    val data: DatabaseMetaData = conn.getMetaData;
    
    val tables: ResultSet = data.getTables(null, "%", "%", List("TABLE").toArray);

    var returnTables = List[TableModel]()
    
    while (tables.next) {
        val tableModel = parseTable(tables, data);    
        val pk = parsePrimaryKeys(tableModel, data)
        val indexes = parseIndexes(tableModel, data);
        val unique = parseUnique(tableModel, data);
                
        returnTables = (returnTables ++ List(tableModel)).toList
    }
    returnTables
  }
  
  
  def parse(connectionString: String): DatabaseModel = {
    new DatabaseModel("database", search(connectionString))
  }  
}