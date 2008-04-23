package ru.yandex.mysqlDiff.diff.simple;

import ru.yandex.mysqlDiff.model._
import java.util.regex.Pattern


object ContentType extends Enumeration {
  val PRIMARY_KEY, INDEX, KEY, CONSTRAINT, CHECK, FULLTEXT, SPATIAL, UNIQUE, COLUMN, UNKNOWN = Value
}


object SimpleTextHarvester {
  
  private def inQuote(data: String, startCheckPos: int, pos: int): boolean = {
    var i: int = startCheckPos + 1;
    var quoteType: int = 0;
    if (data.charAt(i - 1) == '\'') quoteType = -1;
    if (data.charAt(i - 1) == '"') quoteType = 1;
    
    while (i < pos) {
        if (data.charAt(i) == '\'' && quoteType == 0) quoteType = -1;
        if (data.charAt(i) == '\'' && quoteType == -1 && data.charAt(i - 1) != '\\') quoteType = 0;
        if (data.charAt(i) == '"' && quoteType == 0) quoteType = 1;
        if (data.charAt(i) == '"' && quoteType == 1 && data.charAt(i - 1) != '\\') quoteType = 0;
        i = i + 1;
    }
    !(quoteType == 0);
  }
  
  private def inQuote(data: String, pos: int): boolean = {
    inQuote(data, 0, pos)
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
    result;
  }
  
  
  private def isColumnDefinition(testDef: String): ContentType.Value = {
    var cdef = testDef.trim().toUpperCase();
    if (cdef.startsWith("PRIMARY")) return ContentType.PRIMARY_KEY;
    if (cdef.startsWith("INDEX")) return ContentType.INDEX;
    if (cdef.startsWith("KEY")) return ContentType.KEY;
    if (cdef.startsWith("CONSTRAINT")) return ContentType.CONSTRAINT;
    if (cdef.startsWith("CHECK")) return ContentType.CHECK;
    if (cdef.startsWith("FULLTEXT")) return ContentType.FULLTEXT;
    if (cdef.startsWith("SPATIAL")) return ContentType.SPATIAL;
    if (cdef.startsWith("UNIQUE")) return ContentType.UNIQUE;
    if (cdef.matches("[\\w\\_\\-`]+ [\\w\\(\\)]+[\\w\\W]*")) return ContentType.COLUMN
    ContentType.UNKNOWN
  }  
  
  
  private def parseDataTypeDefition(x: String): Tuple3[DataType, int, int] = {
    val witeSpaces = "[\\n\\s]*"
    val nameDefinition = "[\\w\\-\\_]+"
    val UNSIGNED = "unsigned"
    val ZEROFILL = "zerofill"
    val CHARACTER_SET = "character" + witeSpaces + "set" + witeSpaces + "([\\w\\'\\-\\_]+)";
    val COLLATE = "collate" + witeSpaces + "(" + nameDefinition + ")";
    val p = Pattern.compile("(" + nameDefinition + ")" + witeSpaces + "(\\([\\d\\n\\s,]*\\))?" + witeSpaces + "("
            + UNSIGNED + ")?" + witeSpaces + "(" + ZEROFILL + ")?" + witeSpaces + "(" + CHARACTER_SET + ")?"
            + witeSpaces + "(" + COLLATE + ")?",
            Pattern.CASE_INSENSITIVE)    
    val m = p.matcher(x)
    if (!m.find) return null

    var length: Option[Int] = None;
    
    if (m.group(2) != null) {
      val lengthStr = m.group(2).replaceAll("[\\s\\n]*", "")
      val endPos = Math.max(lengthStr.indexOf(','), lengthStr.indexOf(')'))
      length = Some(Integer.parseInt(lengthStr.substring(1, endPos)))
    }
    val dataType = new DataType(m.group(1), length)
    
    dataType.isZerofill = m.group(4) != null
    dataType.isUnsigned = m.group(3) != null
    dataType.characterSet = m.group(6)
    dataType.collate = m.group(8)
    new Tuple3(dataType, m.start, m.end)
  }
  
  private def parseColumnDefinition(x: String): ColumnModel = {
    val columnPattern = Pattern.compile("[\\s\\n]*([\\w`\\-\\_]+) ([\\w\\W]*)?? ")
    val m = columnPattern.matcher(x)
    var result: ColumnModel = null
    if (m.find) {
      val dataType = parseDataTypeDefition(m.group(2));
      if (dataType != null) result = new ColumnModel(m.group(1), dataType._1)
    }
    if (result != null) {
    
      val isNotNullPattern = Pattern.compile("not[\\s\\n]+null", Pattern.CASE_INSENSITIVE);
      val notNullMatcher = isNotNullPattern.matcher(x)
      var stopSearch = false;
      while (notNullMatcher.find && !stopSearch) {
         if (!inQuote(x, notNullMatcher.start)) {
             result.isNotNull = true;
             stopSearch = true
         }
      }
      val autoIncreamentPattern = Pattern.compile("AUTO_INCREMENT", Pattern.CASE_INSENSITIVE)
      val autoMatcher = autoIncreamentPattern.matcher(x);
      stopSearch = false;
      while (autoMatcher.find && !stopSearch) {
        if (!inQuote(x, autoMatcher.start)) {
          result.isAutoIncrement = true;
          stopSearch = true
        }
      }
      
      val commentPattern = Pattern.compile("comment[\\s\\n]+'([\\w\\W]*)'", Pattern.CASE_INSENSITIVE)
      val commentMatcher = commentPattern.matcher(x)
      stopSearch = false;
      while (commentMatcher.find && !stopSearch) {
        if (!inQuote(x, commentMatcher.start)) {
          result.comment = commentMatcher.group(1)
          stopSearch = true
        }
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
                    if (isColumnDefinition(x) == ContentType.COLUMN) {
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
