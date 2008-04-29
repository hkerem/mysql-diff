package ru.yandex.mysqlDiff.diff;


import ru.yandex.mysqlDiff.model._

object SimpleDiffMaker

abstract class AbstractDiffMaker(val from: SqlObjectType, val to: SqlObjectType) {
    type S = SqlObjectType
    type AddDiffFunction = (DiffType[SqlObjectType]) => boolean;
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

        if (bothIsNull) return false
        if (isNameDiff) x(NameDiff(from, to))
        return true;
    }

    def bothIsNull = from == null && to == null
    def fromIsNull = from == null && to != null
    def toIsNull = to == null && from != null
    def isNameDiff =  (from.name != null && !from.name.equals(to.name)) || (from.name == null && to.name != null)
}


trait ListDiffMaker {
    def doListDiff[A <: SqlObjectType](from: Seq[A], to: Seq[A], x: (Option[A], Option[A]) => unit) = {
        val toMap: scala.collection.Map[String, A] = Map(to.map(o => (o.name, o)): _*)
        val fromMap: scala.collection.Map[String, A] = Map(from.map(o => (o.name, o)): _*)
        val bothObject: Seq[(A, A)] = List(to.filter(o => fromMap.contains(o.name)).map(o => (o, fromMap.get(o.name).get)): _*);

        val fromNull = toMap.filterKeys(o => !fromMap.keySet.contains(o)).values //todo Map.excl use insted
        val toNull = fromMap.filterKeys(o => !toMap.keySet.contains(o)).values //todo Map.excl use insted
           for ((a, b) <- bothObject) x(Some(b),Some(a))
        for (a <- fromNull) x(None, Some(a))
        for (a <- toNull) x(Some(a), None)
        true
    }
}


trait StringListDiffMaker {
    def doStringListDiff(from: Seq[String], to: Seq[String], x: (Option[String], Option[String]) => boolean) = {
        val fromSet: Set[String] = Set(from: _*)
        val toSet: Set[String] = Set(to: _*)

        var fullSet = fromSet ++ toSet

        var resume = true

        if (!(fromSet.size == toSet.size && fromSet.subsetOf(toSet) && toSet.subsetOf(fromSet))) {
            fullSet.foreach(e => if (resume) {
                var oFrom: Option[String] = None
                if (fromSet.contains(e)) oFrom = Some(e)
                    var oTo: Option[String] = None
                if (toSet.contains(e)) oTo = Some(e)
                resume = x(oFrom, oTo)
            })
        }
    }
}


class PrimaryKeyDiffMaker(override val from: PrimaryKeyModel, override val to: PrimaryKeyModel)
    extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType) with StringListDiffMaker
{
    override def doDiff(x: AddDiffFunction) = {
        if (super.doDiff(x)) {
            doStringListDiff(from.columns, to.columns, (a, b) => {if (!a.isDefined || !b.isDefined) {x(PrimaryKeyDiff(from, to)); false} else true})
            true
        } else
            false
    }
}


class IndexDiffMaker(override val from: IndexModel, override val to: IndexModel)
    extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType) with StringListDiffMaker
{
    override def doDiff(x: AddDiffFunction) = {
        if (super.doDiff(x)) {
            doStringListDiff(from.columns, to.columns, (a, b) => {if (!a.isDefined || !b.isDefined) {x(IndexKeyDiff(from, to)); false} else true})
            if (from.isUnique != to.isUnique) x(UniqueKeyDiff(from, to))
            true
        } else
            false
    }
}


class ColumnDiffMaker(override val from: ColumnModel, override val to: ColumnModel)
    extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType) with ListDiffMaker
{
    def doColumnDiff(x: AddDiffFunction): boolean = {
        if (!super.doDiff(x)) false
           else
        {
            if (isTypeDiff) x(DataTypeDiff(from, to));
            if (isNullDiff) x(NotNullDiff(from, to));
            if (isAutoIncrementDiff) x(AutoIncrementDiff(from, to))
                else
            true
       }
    }

    override def doDiff(x: AddDiffFunction) = {
        if (!doColumnDiff(x)) false
//todo con diff
        true
    }

    def isAutoIncrementDiff: boolean  = from.isAutoIncrement != to.isAutoIncrement

    def isTypeDiff:boolean = {
        if (from.dataType == to.dataType) return false
        if ((from.dataType == null || to.dataType == null) && from.dataType != to.dataType) return true
        if (from.dataType.name == to.dataType.name && from.dataType.name == null) return false
        if ((from.dataType.name == null || to.dataType.name == null) && from.dataType.name != to.dataType.name) return true
        if (!from.dataType.name.equalsIgnoreCase(to.dataType.name)) return true
        if (from.dataType.isZerofill != to.dataType.isZerofill) return true
        if (from.dataType.isUnsigned != to.dataType.isUnsigned) return true
        !(from.dataType.length.getOrElse(-1) == to.dataType.length.getOrElse(-1))
    }
    def isNullDiff = !(from.isNotNull == to.isNotNull)
}

class TableDiffMaker(override val from: TableModel, override val to: TableModel)
    extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType)
    with ListDiffMaker
{
    def doTableDiff(x: AddDiffFunction):boolean  = true

    override def doDiff(x: AddDiffFunction): boolean = {
        var internalDiff = List[DiffType[SqlObjectType]]();

        def tmpX: AddDiffFunction = o => {
            val sq = internalDiff ++ List(o)
            internalDiff = List(sq: _*)
            true
        }

        if (doTableDiff(tmpX)) {
            doListDiff[ColumnModel](from.columns, to.columns, (from, to) => {
                if (!from.isDefined && to.isDefined) tmpX(new FromIsNull(null, to.get))
                    else
                    if (!to.isDefined && from.isDefined) tmpX(new ToIsNull(from.get, null))
                        else
                        if (to.isDefined && from.isDefined) {
                            val c = new ColumnDiffMaker(from.get, to.get)
                            c.doDiff(tmpX);
                        }
                })

        val pkDiffMaker = new PrimaryKeyDiffMaker(from.primaryKey, to.primaryKey)
        pkDiffMaker.doDiff(tmpX)

        doListDiff[IndexModel](from.keys, to.keys, (from, to) => {
            if (!from.isDefined && to.isDefined) tmpX(new FromIsNull(null, to.get))
                else
                  if (!to.isDefined && from.isDefined) {
                    tmpX(new ToIsNull(from.get, null))
                  } else
                        if (to.isDefined && from.isDefined) {
                            val idxDiffMaker = new IndexDiffMaker(from.get, to.get)
                            idxDiffMaker.doDiff(tmpX)
                        }
            })
        }
        x(TableDiff(from, to, internalDiff))
    }
}


class DatabaseDiffMaker(override val from: DatabaseModel, override val to: DatabaseModel)
    extends NameDiffMaker(from: SqlObjectType, to: SqlObjectType)
    with ListDiffMaker
{
    override def doDiff(x :AddDiffFunction): boolean = {
        var internalDiff = List[DiffType[SqlObjectType]]();

        def tmpX: AddDiffFunction = o => {
            val sq = internalDiff ++ List(o)
            internalDiff = List(sq: _*)
            true
        }

        doListDiff[TableModel](from.declarations, to.declarations, (from, to) => {
            if (!from.isDefined && to.isDefined) {
                tmpX(new FromIsNull(null, to.get));
            } else
                if (!to.isDefined && from.isDefined) {
                    tmpX(new ToIsNull(from.get, null));
                } else
                    if (to.isDefined && from.isDefined) {
                    val c = new TableDiffMaker(from.get, to.get)
                    c.doDiff(tmpX);
                }
            })
        x(DatabaseDiff(from, to, internalDiff))
    }
}

