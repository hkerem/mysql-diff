package ru.yandex.mysqlDiff.diff.simple;

import ru.yandex.mysqlDiff.model._

object SimpleScriptBuilder {
  def getString(x: DiffType[SqlObjectType]):String = x match {
    case NameDiff(a,b) => {
      ""//nothing
    }
    case DataTypeDiff(a, b) => {
      var typeLength: String = "";
      if (b.asInstanceOf[DataType].length.isDefined) 
        typeLength = "(" + b.asInstanceOf[DataType].length.get + ")"
        else
          typeLength = ""
      if (a.name.equals(b.name)) 
           "ALTER TABLE " + b.asInstanceOf[DataType].parent.parent.name + " MODIFY " + b.asInstanceOf[DataType].parent.name + " " + b.asInstanceOf[DataType].name + typeLength + "";
      else
        "ALTER TABLE " + b.asInstanceOf[DataType].parent.parent.name + " CHANGE " + a.asInstanceOf[DataType].parent.name + " "  + b.asInstanceOf[DataType].parent.name + "" + b.asInstanceOf[DataType].name + typeLength + "";
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
