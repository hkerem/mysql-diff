package ru.yandex.mysqlDiff.diff.simple;

import ru.yandex.mysqlDiff.model._

object SimpleScriptBuilder {
  
  
  private def makeDiffForColumn(a: SqlObjectType, b: SqlObjectType): String = {
    val A = a.asInstanceOf[ColumnModel]
    val B = b.asInstanceOf[ColumnModel]

    val aType = A.dataType
    val bType = B.dataType
    
    var typeLength: String = "";
    if (bType.length.isDefined) 
      typeLength = "(" + bType.length.get + ")"
      else
        typeLength = ""
    
    var notNullDef = ""
    if (B.isNotNull) notNullDef = " NOT NULL ";
          
    if (B.name.equals(A.name)) 
         "ALTER TABLE " + B.parent.name + " MODIFY COLUMN " + B.name + " " + bType.name + typeLength + notNullDef + ";";
    else
      "ALTER TABLE " + A.parent.name + " CHANGE COLUMN " + A.name + " "  + B.name + " " + bType.name + typeLength + notNullDef + ";";
  } 
  
  def getString(x: DiffType[SqlObjectType]):String = x match {
    
  case DatabaseDiff(a, b, diffList) => {
    var result = ""
    for (x <- diffList) {
      result = result + getString(x) + "\n"
    }
    return result    
  }
  
  case NameDiff(a,b) => {
      ""//nothing
    }
    case DataTypeDiff(a, b) => {
      makeDiffForColumn(a, b)
    }
    
    case NotNullDiff(a, b) => {
      makeDiffForColumn(a, b)
    }

    case ToIsNull(a, b) => {
      a match {
        case TableModel(name, columns) => {
          "DROP TABLE " + name + ";"
        }
        case ColumnModel(name, dataType) => {
          "ALTER TABLE " + a.asInstanceOf[ColumnModel].parent.name + " DROP COLUMN " + name + ";"
        }
      }
    }
    
    case FromIsNull(a, b) => {
      b match {
        case TableModel(name, columns) => {
          b.asInstanceOf[TableModel].toCreateStatement
        }
        case ColumnModel(name, dataType) => {
          "ALTER TABLE " + b.asInstanceOf[ColumnModel].parent.name + " ADD COLUMN " + b.toCreateStatement + ";"
        }
      }
    }
    
    case TableDiff(a, b, diffList) => {
      var result = ""
      for (x <- diffList) {
        result = result + getString(x) + "\n"
      }
      return result
    }
  }
}
