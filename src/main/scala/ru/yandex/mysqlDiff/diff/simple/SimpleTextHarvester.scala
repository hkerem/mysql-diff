package ru.yandex.mysqlDiff.diff.simple;

import ru.yandex.mysqlDiff.model._
import java.util.regex.Pattern

class SimpleTextHarvester {
  
  def parseCreateTable(tableCreate: String): TableModel = {
    var data = tableCreate.trim()
    var i = 0
    
    while ((data.charAt(i) != '(') && (data.charAt(i) != ' ')) {
      i = i + 1;
    }
    
    var tableName = data.substring(0, i).trim()
    if (tableName.startsWith("`")) tableName = tableName.substring(1, tableName.length - 1)
    
    var columnsDiffinition = data.substring(i).trim();
    if (columnsDiffinition.startsWith("(")) columnsDiffinition = columnsDiffinition.substring(1, columnsDiffinition.length - 1);
    
    //val tableModel = new TableModel(tableModel);
    null
  }
  
  
  def models(data: String): Seq[TableModel] = {
    val p = Pattern.compile("CREATE TABLE", Pattern.CASE_INSENSITIVE);
    val createStatements = p.split(data);
    var result = List[TableModel]();
    for (val x <- createStatements) {
      if (!x.trim().equals("")) {
        val t: TableModel = parseCreateTable(x.trim());
        
        result = List(result ++ List(t): _*);
      }
    }
    result
  }
  
}
