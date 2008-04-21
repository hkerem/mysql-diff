package ru.yandex.mysqlDiff.diff.simple;

import ru.yandex.mysqlDiff.model._
import java.util.regex.Pattern

object SimpleTextHarvester {
  
  private def inQuote(data: String, pos: int): boolean = {
    var i: int = 1;
    var quoteType: int = 0;
    if (data.charAt(0) == '\'') quoteType = -1;
    if (data.charAt(0) == '"') quoteType = 1;
    
    while (i < pos) {
        if (data.charAt(i) == '\'' && quoteType == 0) quoteType = -1;
        if (data.charAt(i) == '\'' && quoteType == -1 && data.charAt(i - 1) != '\\') quoteType = 0;
        if (data.charAt(i) == '"' && quoteType == 0) quoteType = 1;
        if (data.charAt(i) == '"' && quoteType == 1 && data.charAt(i - 1) != '\\') quoteType = 0;
        i = i + 1;
    }
    return !(quoteType == 0);
  }  


  
  private def findStopBraketsPos(data: String, startBracketsPos: int): int = {
    var countOfBrakets = 1;

    var i = startBracketsPos + 1;
    var quoteType = 0;
    
    if (data.charAt(0) == '\'') quoteType = -1;
    if (data.charAt(0) == '"') quoteType = 1;
    while (i < data.length()) {
        if (data.charAt(i) == '\'' && quoteType == 0) quoteType = -1;
        if (data.charAt(i) == '\'' && quoteType == -1 && data.charAt(i - 1) != '\\') quoteType = 0;
        if (data.charAt(i) == '"' && quoteType == 0) quoteType = 1;
        if (data.charAt(i) == '"' && quoteType == 1 && data.charAt(i - 1) != '\\') quoteType = 0;
        if (quoteType == 0 && data.charAt(i) == ')') countOfBrakets = countOfBrakets - 1;
        if (quoteType == 0 && data.charAt(i) == '(') countOfBrakets = countOfBrakets + 1;
        if (countOfBrakets == 0) return i;
        i = i + 1;
    }
    return -1;
  }
  
  private def splitByComma(data: String): Seq[String] = {
       var result = List[String]()
       var countOfBrakets = 0;
       var lastCommaPos = 0;
       var i = 0;
       var quoteType = 0;
       if (data.charAt(0) == '\'') quoteType = -1;
       if (data.charAt(0) == '"') quoteType = 1;
       while (i < data.length()) {
         if (data.charAt(i) == '\'' && quoteType == 0) quoteType = -1;
         if (data.charAt(i) == '\'' && quoteType == -1 && data.charAt(i - 1) != '\\') quoteType = 0;
         if (data.charAt(i) == '"' && quoteType == 0) quoteType = 1;
         if (data.charAt(i) == '"' && quoteType == 1 && data.charAt(i - 1) != '\\') quoteType = 0;
         if (quoteType == 0 && data.charAt(i) == ')') countOfBrakets = countOfBrakets - 1;
         if (quoteType == 0 && data.charAt(i) == '(') countOfBrakets = countOfBrakets + 1;
         if (quoteType == 0 && countOfBrakets == 0 && data.charAt(i) == ',') 
         {
                val uncommedText = data.substring(lastCommaPos + 1, i)
                val sq: Seq[String] = result ++ List(uncommedText)
                result = List(sq: _*)
                lastCommaPos = i;
         }
         i = i + 1;
       }
    val uncommedText = data.substring(lastCommaPos + 1) 
    val sq = result ++ List(uncommedText)
    result = List(sq: _*)
    return result;
  }
  
  
  private def isColumnDefinition(testDef: String): boolean = {
    var cdef = testDef.trim().toUpperCase();
    if (cdef.startsWith("PRIMARY")) return false;
    if (cdef.startsWith("INDEX")) return false;
    if (cdef.startsWith("KEY")) return false;
    if (cdef.startsWith("CONSTRAINT")) return false;
    if (cdef.startsWith("CHECK")) return false;
    if (cdef.startsWith("FULLTEXT")) return false;
    if (cdef.startsWith("SPATIAL")) return false;
    if (cdef.startsWith("UNIQUE")) return false;
    return cdef.matches("[\\w\\_\\-`]+ [\\w\\(\\)]+[\\w\\W]*");
  }  
  
  
  private def parseDataTypeDefition(x: String): DataType = {
    var dataTypeStr = x.trim.toUpperCase
    if (dataTypeStr.matches("[\\w-\\_]+[\\s]+[\\w]+[\\w\\W]*"))
      new DataType(dataTypeStr.split(" ")(0), None)
      else
      {
        val p = Pattern.compile("([\\w_-`]+)[\\s\\n]*\\((([\\d]+)([\\s\\n]*\\,[\\s\\n]*([\\d]+))?)\\)")
        val m = p.matcher(dataTypeStr);
        if (m.find) 
          new DataType(m.group(1), Some(0 + Integer.parseInt(m.group(3))));
        else
          null
      }
  }
  
  private def parseColumnDefinition(x: String): ColumnModel = {
    val columnPattern = Pattern.compile("[\\s\\n]*([\\w`\\-\\_]+) ([\\w\\s\\n\\(\\),]+)[\\w\\W]*")
    val m = columnPattern.matcher(x)
    var result: ColumnModel = null
    if (m.find) {
      val dataType = parseDataTypeDefition(m.group(2));
      if (dataType != null) result = new ColumnModel(m.group(1), dataType)
    }
    val isNotNullPattern = Pattern.compile("not[\\s\\n]+null", Pattern.CASE_INSENSITIVE);
    val notNullMatcher = isNotNullPattern.matcher(x)
    var stopSearch = false;
    while (notNullMatcher.find && !stopSearch && result != null) {
      if (!inQuote(x, notNullMatcher.start)) {
        result.isNotNull = true;
        stopSearch = true
      }
    } 
    result
  }
  
  
  def search(inputData: String): Seq[TableModel] = {
    var data = inputData.replaceAll("--[\\w\\W]*?\n", "")
    val p = Pattern.compile("CREATE[\\s\\n]+TABLE", Pattern.CASE_INSENSITIVE);
    val tableNamePattern = Pattern.compile("[\\s\\n]+([\\w`%]+)[\\s\\n]+\\(");

    val matcher = p.matcher(data)
        
    var result = List[TableModel]()
    
    while (matcher.find) {
        val startPos = matcher.start();
        if (!inQuote(data, startPos)) {
            var tableName = "";
            
            var createTableData = data.substring(startPos);
            val tableNameMatcher = tableNamePattern.matcher(createTableData);
            if (tableNameMatcher.find()) {
                tableName = tableNameMatcher.group(1).trim();
            }
           
            if (!tableName.equals("")) {
              val startBracketsPos = createTableData.indexOf('(');
              val stopBraketsPos = findStopBraketsPos(createTableData, startBracketsPos);
              if (startBracketsPos != -1) {
                val definitions = splitByComma(createTableData.substring(startBracketsPos + 1, stopBraketsPos));
                if (definitions.size > 0) {
                  var columns = List[ColumnModel]();
                  
                  
                  for (x <- definitions) {
                    if (isColumnDefinition(x)) {
                      var cModel = parseColumnDefinition(x)
                      if (cModel != null) {
                        val sqc = columns ++ List(cModel)
                        columns = List(sqc: _*)
                      }
                    }
                  }

                  var tableModel = new TableModel(tableName, columns);
                  for (x <- columns) {
                    x.parent = tableModel
                  }
                  
                  val sqr = result ++ List(tableModel)
                  result = List(sqr: _*)
                }
              }
            }
        }
    }
    result
}  
  
  
  
  def parse(inputData: String): DatabaseModel = {
    //comments remove
    var data = inputData.replaceAll("\\-\\-[\\w\\W]*?\n", "");
    new DatabaseModel("database", search(data))
  }

  
}
