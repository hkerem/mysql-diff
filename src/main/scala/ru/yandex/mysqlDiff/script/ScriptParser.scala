package ru.yandex.mysqlDiff.script

import ru.yandex.mysqlDiff.model._
import java.util.regex.Pattern


object ContentType extends Enumeration {
    val PRIMARY_KEY, INDEX, KEY, CONSTRAINT, CHECK, FULLTEXT, SPATIAL, UNIQUE, COLUMN, UNKNOWN = Value
}


object ScriptParser {
  
    private def inQuote(data: String, startCheckPos: Int, pos: Int): Boolean =
    {
        var i: Int = startCheckPos + 1
        var quoteType: Int = 0;
        if (data.charAt(i - 1) == '\'') quoteType = -1
        if (data.charAt(i - 1) == '"') quoteType = 1
    
        while (i < pos) {
            if (data.charAt(i) == '\'' && quoteType == 0) quoteType = -1
            if (data.charAt(i) == '\'' && quoteType == -1 && data.charAt(i - 1) != '\\') quoteType = 0

            if (data.charAt(i) == '"' && quoteType == 0) quoteType = 1;
            if (data.charAt(i) == '"' && quoteType == 1 && data.charAt(i - 1) != '\\') quoteType = 0
            i = i + 1
        }
        !(quoteType == 0)
    }
  
    private def inQuote(data: String, pos: Int): Boolean = {
        inQuote(data, 0, pos)
    }


  
     private def findStopBraketsPos(data: String, startBracketsPos: Int): Int = {
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
                var uncommedText = data.substring(lastCommaPos, i).trim
                if (uncommedText.startsWith(",")) uncommedText = uncommedText.substring(1)
                val sq: Seq[String] = result ++ List(uncommedText)
                result = sq.toList
                lastCommaPos = i;
            }
            i = i + 1;
        }
        val uncommedText = data.substring(lastCommaPos + 1)
        (result ++ List(uncommedText)).toList
    }
  
  
    private def getDefinitionType(testDef: String): ContentType.Value = {
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

    private def parseDataTypeDefition(x: String): Tuple3[DataType, Int, Int] = {
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

    private def parseColumnDefinition(data: String): ColumnModel = {
        var x = data.trim
        val columnPattern = Pattern.compile("[\\s\\n]*([\\w`\\-\\_]+)[\\s\\n]+([\\w\\W]*)?")
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

            val primaryKeyPattern = Pattern.compile("PRIMARY[\\s\\n]+KEY", Pattern.CASE_INSENSITIVE)
            val primaryKeyMatcher = primaryKeyPattern.matcher(x)
            stopSearch = false
            while (primaryKeyMatcher.find && !stopSearch) {
                if (!inQuote(x, primaryKeyMatcher.start)) {
                    result.primaryKey = true
                   stopSearch = true;
                }
            }

            val defaultPattern = Pattern.compile("DEFAULT[\\s\\n]+(('[\\w\\W]*')|([\\d\\-\\,\\.]+)|(NOW[\\s]\\(\\)))", Pattern.CASE_INSENSITIVE)
            val defaultMatcher = defaultPattern.matcher(x)
            stopSearch = false
            while (defaultMatcher.find && !stopSearch) {
                if (!inQuote(x, defaultMatcher.start)) {
                    result.defaultValue = defaultMatcher.group(1)
                    stopSearch = true
                }
            }
      
            val uniquePattern = Pattern.compile("UNIQUE", Pattern.CASE_INSENSITIVE)
            val uniqueMatcher = uniquePattern.matcher(x)
            stopSearch = false
            while (uniqueMatcher.find && !stopSearch) {
                if (!inQuote(x, uniqueMatcher.start)) {
                    result.isUnique = true
                    stopSearch = true
                }
            }
      
            if (!result.primaryKey && !result.isUnique) {
                val indexPattern = Pattern.compile("KEY", Pattern.CASE_INSENSITIVE)
                val indexMatcher = indexPattern.matcher(x)
                stopSearch = false;
                while (indexMatcher.find && !stopSearch) {
                        if (!inQuote(x, indexMatcher.start)) {
                            result.isIndex = true
                            stopSearch = true
                        }
                }
            }
        }
        result
    }
  
  
  
  
  
  
    private def parsePrimaryKeyDefinition(x: String): PrimaryKey = {
        val bStart = x.indexOf('(') + 1
        val bEnd = x.indexOf(')')
        val data = x.substring(bStart, bEnd).trim();
        var columns = Set[String]()
        for (colName <- data.split(",")) columns = columns + (colName.trim())

        new PrimaryKey("", List(columns.toArray: _*))
    }
  
    private def parseKeyKeyDefinition(x: String): IndexModel = {
        parseKeyDefinition("KEY", x)
    }
  
    private def parseIndexKeyDefinition(x: String): IndexModel = {
         parseKeyDefinition("INDEX", x)
    }
  
    private def parseUniqueKeyDefinition(x: String): IndexModel = {
        val indexPattern = Pattern.compile("UNIQUE[\\s\\n]+((INDEX)|(KEY))?[\\s\\n]+([\\w\\-\\_]+)?[\\s\\n]+(USING[\\s\\n]+[\\w\\-\\_][\\n\\s]+)?[\\n\\s]*\\(([\\w\\-\\_\\s\\n,\\'\\`]+)\\)", Pattern.CASE_INSENSITIVE)
        val matcher = indexPattern.matcher(x)
        if (matcher.find) {
            var name = ""
                if (matcher.group(4) != null)
                    name = matcher.group(4)
                else
                    name = ""

            val typeName = matcher.group(5)
            val columnNames = matcher.group(6).split(",").map(x => x.trim)
            new IndexModel(name, columnNames, true)
        } else
            null
    }
  
    private def parseKeyDefinition(tp:String, x: String): IndexModel = {
        val indexPattern = Pattern.compile(tp + "[\\s\\n]+([\\w\\-\\_]+)?[\\s\\n]+(USING[\\s\\n]+[\\w\\-\\_]+[\\n\\s]+)?[\\n\\s]*\\(([\\w\\-\\_\\s\\n,\\'\\`]+)\\)", Pattern.CASE_INSENSITIVE)
        val matcher = indexPattern.matcher(x)
        if (matcher.find) {
            var name = ""
            if (matcher.group(1) != null)
                name = matcher.group(1)
            else
                name = ""
            
            val typeName = matcher.group(2)
            val columnNames = matcher.group(3).split(",").map(x => x.trim)
            new IndexModel(name, columnNames, false)
        } else
            null
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

                if (tableNameMatcher.find()) tableName = tableNameMatcher.group(1).trim();
           
                if (!tableName.equals("")) {
                    val startBracketsPos = createTableData.indexOf('(');
                    val stopBraketsPos = findStopBraketsPos(createTableData, startBracketsPos);

                    if (startBracketsPos != -1 && stopBraketsPos != -1) {
                        var tableStringDiff = createTableData.substring(startBracketsPos, stopBraketsPos).trim
                        if (tableStringDiff.startsWith("(")) tableStringDiff = tableStringDiff.substring(1)
                        val definitions = splitByComma(tableStringDiff);

                        if (definitions.size > 0) {
                            var columns = List[ColumnModel]();
                            var primaryKey: PrimaryKey = null;
                            var indexes =  List[IndexModel]();

                            definitions.foreach(x => {
                                val defType = getDefinitionType(x);
                                if (defType == ContentType.COLUMN) {
                                    var cModel = parseColumnDefinition(x)
                                    if (cModel != null) {
                                        columns = (columns ++ List(cModel)).toList
                                        if (cModel.isUnique) {
                                            indexes = (indexes ++ List(new IndexModel(cModel.name, List(cModel.name), true))).toList
                                        }
                                        if (cModel.isIndex) {
                                            indexes = (indexes ++ List(new IndexModel(cModel.name, List(cModel.name), false))).toList
                                        }
                                    }
                                }

                                if (defType == ContentType.PRIMARY_KEY) {
                                    primaryKey = parsePrimaryKeyDefinition(x)
                                }
                    
                                if (defType == ContentType.INDEX) {
                                    val idx = parseIndexKeyDefinition(x)
                                    if (idx != null) indexes = (indexes ++ List(idx)).toList
                                }
                    
                                if (defType == ContentType.KEY) {
                                    val idx = parseKeyKeyDefinition(x)
                                    if (idx != null) indexes = (indexes ++ List(idx)).toList
                                }

                                if (defType == ContentType.UNIQUE) {
                                    val idx = parseUniqueKeyDefinition(x)
                                    if (idx != null) indexes = (indexes ++ List(idx)).toList
                                }
                                //todo other def
                            })

                            var tableModel = new TableModel(tableName, columns);
                  
                            if (primaryKey != null) {
                                tableModel.primaryKey = Some(primaryKey)
                            } else {
                                val primaryKeyColumns = columns.filter(x => x.primaryKey)
                                if (primaryKeyColumns.size > 0) {
                                    tableModel.primaryKey = Some(new PrimaryKey("", primaryKeyColumns.map(x => x.name)))
                                }
                            }
                            tableModel.keys = indexes
                            result = (result ++ List(tableModel)).toList
                        }
                    }
                }
            }
        }
        result
    }

    def parseModel(scriptString: String) = {
        val script = parse(scriptString)
        new DatabaseModel("database", script.stmts.map(_.asInstanceOf[CreateTableStatement].model))
    }
    
    def parse(scriptString: String): Script = {
        // remove comments
        var data = scriptString.replaceAll("\\-\\-[\\w\\W]*?\n", "");
        new Script(search(data).map(CreateTableStatement(_)))
   	}
}

// vim: set ts=4 sw=4 et:
