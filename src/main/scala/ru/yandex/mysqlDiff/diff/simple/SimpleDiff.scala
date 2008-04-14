package ru.yandex.mysqlDiff.diff.simple;

object SimpleDiffMaker {

}

abstract class AbstractDiffMaker[A >: SqlObjectType](val from: A, val to: A) {
  def doDiff;
  def isNameDiff: boolean  = (from.name != null && from.equals(to.name)) || from.name == to.name
}


case class ColumnDiffMaker[ColumnModel](val from: ColumnModel, val to: ColumnModel) 
        extends AbstractDiffMaker {
  
  def doDiff(x: (ColumnDiff) => unit) = {
    if (isNameDiff) x(NameDiff(from.name, to.name))
    if (isTypeDiff) x(DataTypeDiff(from.dataType, to.dataType))
    if (isNotNullDiff) x(NotNullDiff(from.isNotNull, to.isNotNull))    
  }
  
  def isTypeDiff: boolean = (from.dataType != null && from.dataType.equals(to.dataType)) || from.dataType == to.DataType
  def isNotNullDiff: boolean = from.isNotNull == to.isNotNull
}



class MapDiffMaker[A](val from: Map[String, A], val to: Map[String, A]) {
  
}

case class TableDiff(val from: TableModel, val to: TableModel) extends AbstractDiffMaker {
  def doDiff: TableDiff = {
    return null
  }
}