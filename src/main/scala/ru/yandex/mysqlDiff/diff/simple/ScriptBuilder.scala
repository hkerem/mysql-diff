package ru.yandex.mysqlDiff.output;

import ru.yandex.mysqlDiff.model._

class SimpleScriptBuilder {
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
      ""
    }
    
    case FromIsNull(a, b) => {
      ""
    }
    
    case TableDiff(a, b, diffList) => {
      ""
    }
  }
}
