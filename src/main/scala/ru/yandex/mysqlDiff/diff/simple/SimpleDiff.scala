package ru.yandex.mysqlDiff.diff.simple;


import ru.yandex.mysqlDiff.model._

object SimpleDiffMaker {

}

abstract class AbstractDiffMaker(val from: SqlObjectType, val to: SqlObjectType) {
  type S = SqlObjectType
  type AddDiffFunction = (DiffType[S]) => boolean;
  def doDiff(x: AddDiffFunction): boolean;
}


class NameDiffMaker(override val from: SqlObjectType, override val to: SqlObjectType) 
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
  def isNameDiff =  (from.name != null && !from.name.equals(to.name)) || (from.name == null && to.name != null)
}


trait ListDiffMaker {
  def doListDiff[A <: SqlObjectType](from: Seq[A], to: Seq[A], x: (Option[A], Option[A]) => unit) = {
    val toMap: scala.collection.Map[String, A] = Map(to.map(o => (o.name, o)): _*)
    val fromMap: scala.collection.Map[String, A] = Map(from.map(o => (o.name, o)): _*)

    val bothObject: Seq[(A, A)] = List(to.filter(o => fromMap.contains(o.name)).map(o => (o, fromMap.get(o.name).get)): _*);
    
    val fromNull = toMap.filterKeys(o => fromMap.keySet.contains(o)).values //todo Map.excl use insted
    val toNull = fromMap.filterKeys(o => toMap.keySet.contains(o)).values //todo Map.excl use insted
    
    for ((a, b) <- bothObject) x(Some(a),Some(b))
    for (a <- fromNull) x(None, Some(a))
    for (a <- toNull) x(Some(a), None)
    true
  }
}



class ColumnDiffMaker(override val from: ColumnModel, override val to: ColumnModel)
        extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType) with ListDiffMaker 
{
  def doColumnDiff(x: AddDiffFunction): boolean = {
    if (!super.doDiff(x)) 
      false
       else 
       { 
         if (isTypeDiff) x(DataTypeDiff(from, to));
         if (isNullDiff) x(NotNullDiff(from, to));
         else true;        
       }
  }
  
  override def doDiff(x: AddDiffFunction)  = {
    if (!doColumnDiff(x)) false
//todo con diff
    true
  }
  
  def isTypeDiff:boolean = {
    if (from.dataType == to.dataType) return false;
    if ((from.dataType == null || to.dataType == null) && from.dataType != to.dataType) return true;
    if (from.dataType.name == to.dataType.name && from.dataType.name == null) return false;
    if ((from.dataType.name == null || to.dataType.name == null) && from.dataType.name != to.dataType.name) return true;
    if (!from.dataType.name.equals(to.dataType.name)) return true;
    
    !(from.dataType.length.getOrElse(-1) == to.dataType.length.getOrElse(-1))
  }
  def isNullDiff = !(from.isNotNull == to.isNotNull)
}
 
class TableDiffMaker(override val from: TableModel, override val to: TableModel) 
        extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType) 
        with ListDiffMaker
{
  def doTableDiff(x: AddDiffFunction):boolean  = {
    true
  }
  
  override def doDiff(x: AddDiffFunction): boolean = {
    if (doTableDiff(x)) {
      doListDiff[ColumnModel](from.columns, to.columns, (from, to) => {
              val c = new ColumnDiffMaker(from.get, to.get)
              c.doDiff(x);
              })
      true
    } 
    else
      false    
  }
}


