package ru.yandex.mysqlDiff.diff.simple;


import ru.yandex.mysqlDiff.model._


object SimpleDiffMaker {

}

abstract class AbstractDiffMaker(val from: SqlObjectType, val to: SqlObjectType) {
  type S = SqlObjectType
  type AddDiffFunction = (DiffType[S]) => boolean;
  def doDiff(x: AddDiffFunction): boolean;
}


class NameDiff(override val from: SqlObjectType, override val to: SqlObjectType) 
        extends AbstractDiffMaker(from: SqlObjectType, to: SqlObjectType) 
{
  override def doDiff(x: AddDiffFunction): boolean  = {
    if (fromIsNull) {
      x(FromIsNull(from,to))
      return false
    } 
    if (toIsNull) {
      x(ToIsNull(from, to))
      return false
    }
    if (isNameDiff) x(NameDiff(from, to))
    return true;
  }
  def fromIsNull = from == null && to != null
  def toIsNull = to == null && from != null
  def isNameDiff =  (from.name != null && from.name.equals(to.name)) || (from.name == null && to.name != null)
}


class ColumnDiff(override val from: ColumnModel, override val to: ColumnModel)
        extends NameDiff(from: SqlObjectType, to: SqlObjectType) 
{
  override def doDiff(x: AddDiffFunction): boolean  = {
    if (!super.doDiff(x)) return false;
    if (isTypeDiff) x(DataTypeDiff(from, to));
    if (isNullDiff) x(NotNullDiff(from, to));
    return true;
  }
  
  def isTypeDiff = (from.dataType != null && from.dataType.equals(to.name)) || (from.dataType == null && to.dataType != null) 
  def isNullDiff = (from.isNotNull == to.isNotNull)
}