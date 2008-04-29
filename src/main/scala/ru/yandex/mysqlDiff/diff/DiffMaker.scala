package ru.yandex.mysqlDiff.diff


import ru.yandex.mysqlDiff.model._

trait AbstractDiffMaker {
    type AddDiffFunction = (DiffType) => Boolean;
}


trait NameDiffMaker extends AbstractDiffMaker {
    def doNameDiff(from: SqlObjectType, to: SqlObjectType, x: AddDiffFunction): Boolean  = {
        if (fromIsNull(from, to)) {
            x(FromIsNull(from, to))
            return false
        }

        if (toIsNull(from, to)) {
            x(ToIsNull(from, to))
            return false
        }

        if (bothIsNull(from, to)) return false
        if (isNameDiff(from, to)) x(NameDiff(from, to))
        return true;
    }

    def bothIsNull(from: SqlObjectType, to: SqlObjectType) = from == null && to == null
    def fromIsNull(from: SqlObjectType, to: SqlObjectType) = from == null && to != null
    def toIsNull(from: SqlObjectType, to: SqlObjectType) = to == null && from != null
    def isNameDiff(from: SqlObjectType, to: SqlObjectType) =  (from.name != null && !from.name.equals(to.name)) || (from.name == null && to.name != null)
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
    def doStringListDiff(from: Seq[String], to: Seq[String], x: (Option[String], Option[String]) => Boolean) = {
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


object PrimaryKeyDiffMaker extends NameDiffMaker with StringListDiffMaker 
{
    def doDiff(from: PrimaryKeyModel, to: PrimaryKeyModel, x: AddDiffFunction) = {
        if (super.doNameDiff(from, to, x)) {
            doStringListDiff(from.columns, to.columns, (a, b) => {if (!a.isDefined || !b.isDefined) {x(PrimaryKeyDiff(from, to)); false} else true})
            true
        } else
            false
    }
}


object IndexDiffMaker extends NameDiffMaker with StringListDiffMaker
{
    def doDiff(from:IndexModel, to:IndexModel, x: AddDiffFunction) = {
        if (super.doNameDiff(from, to, x)) {
            doStringListDiff(from.columns, to.columns, (a, b) => {if (!a.isDefined || !b.isDefined) {x(IndexKeyDiff(from, to)); false} else true})
            if (from.isUnique != to.isUnique) x(UniqueKeyDiff(from, to))
            true
        } else
            false
    }
}


object ColumnDiffMaker extends NameDiffMaker with ListDiffMaker
{

    def doColumnDiff(from: ColumnModel, to: ColumnModel, x: AddDiffFunction): Boolean = {
        if (!super.doNameDiff(from, to, x)) false
           else
        {
            if (isTypeDiff(from, to)) x(DataTypeDiff(from, to))
            if (isNullDiff(from, to)) x(NotNullDiff(from, to))
            if (isAutoIncrementDiff(from, to)) x(AutoIncrementDiff(from, to))
            if (isDefaultDiff(from, to)) x(DefaultValueDiff(from, to))        
            true
       }
    }

    def doDiff(from: ColumnModel, to: ColumnModel, x: AddDiffFunction) = {
        if (!doColumnDiff(from, to, x)) false
//todo con diff
        true
    }

    def isAutoIncrementDiff(from: ColumnModel, to: ColumnModel): Boolean  = from.isAutoIncrement != to.isAutoIncrement

    def isTypeDiff(from: ColumnModel, to: ColumnModel):Boolean = {
        if (from.dataType == to.dataType) return false
        if ((from.dataType == null || to.dataType == null) && from.dataType != to.dataType) return true
        if (from.dataType.name == to.dataType.name && from.dataType.name == null) return false
        if ((from.dataType.name == null || to.dataType.name == null) && from.dataType.name != to.dataType.name) return true
        if (!from.dataType.name.equalsIgnoreCase(to.dataType.name)) return true
        if (from.dataType.isZerofill != to.dataType.isZerofill) return true
        if (from.dataType.isUnsigned != to.dataType.isUnsigned) return true
        !(from.dataType.length.getOrElse(-1) == to.dataType.length.getOrElse(-1))
    }
    def isNullDiff(from: ColumnModel, to: ColumnModel) = !(from.isNotNull == to.isNotNull)

    def isDefaultDiff(from: ColumnModel, to: ColumnModel) = !(from.defaultValue == to.defaultValue)

}

object TableDiffMaker extends NameDiffMaker with ListDiffMaker {
    def doTableDiff(x: AddDiffFunction):Boolean  = true

    def doDiff(from:TableModel, to: TableModel, x: AddDiffFunction): Boolean = {
        var internalDiff = List[DiffType]();

        def tmpX: AddDiffFunction = o => {
            internalDiff = (internalDiff ++ List(o)).toList 
            true
        }

        if (doTableDiff(tmpX)) {
            doListDiff[ColumnModel](from.columns, to.columns, (from, to) => {
                if (!from.isDefined && to.isDefined) tmpX(new FromIsNull(null, to.get))
                    else
                    if (!to.isDefined && from.isDefined) tmpX(new ToIsNull(from.get, null))
                        else
                        if (to.isDefined && from.isDefined) {
                            ColumnDiffMaker.doDiff(from.get, to.get, tmpX)
                        }
                })

        PrimaryKeyDiffMaker.doDiff(from.primaryKey, to.primaryKey, tmpX)

        doListDiff[IndexModel](from.keys, to.keys, (from, to) => {
            if (!from.isDefined && to.isDefined) tmpX(new FromIsNull(null, to.get))
                else
                  if (!to.isDefined && from.isDefined) {
                    tmpX(new ToIsNull(from.get, null))
                  } else
                        if (to.isDefined && from.isDefined) {
                            IndexDiffMaker.doDiff(from.get, to.get, tmpX)
                        }
            })
        }
        x(TableDiff(from, to, internalDiff))
    }
}


object DatabaseDiffMaker extends NameDiffMaker with ListDiffMaker {
    def doDiff(from: DatabaseModel, to: DatabaseModel, x: AddDiffFunction): Boolean = {
        var internalDiff = List[DiffType]();

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
                        TableDiffMaker.doDiff(from.get, to.get, tmpX)
                    }
            })
        x(DatabaseDiff(from, to, internalDiff))
    }
}

