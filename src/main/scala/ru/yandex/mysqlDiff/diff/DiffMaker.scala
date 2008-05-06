package ru.yandex.mysqlDiff.diff


import ru.yandex.mysqlDiff.model._

object ListDiffMaker {
    def compareSeqs[A, B](a: Seq[A], b: Seq[B], comparator: (A, B) => Boolean): (Seq[A], Seq[B], Seq[(A, B)]) = {
        var onlyInA = List[A]()
        var onlyInB = List[B]()
        var inBothA = List[(A, B)]()
        var inBothB = List[(A, B)]()
        
        for (x <- a) {
            b.find(comparator(x, _)) match {
                case Some(y) => inBothA += (x, y)
                case None => onlyInA += x
            }
        }
        
        for (y <- b) {
            a.find(comparator(_, y)) match {
                case Some(x) => inBothB += (x, y)
                case None => onlyInB += y
            }
        }
        
        (onlyInA, onlyInB, inBothA)
    }
    
    def doListDiff[A <: SqlObjectType](from: Seq[A], to: Seq[A]): Tuple3[Seq[A], Seq[A], Seq[(A, A)]] = {
        val toMap: Map[String, A] = Map(to.map(o => (o.name, o)): _*)
        val fromMap: Map[String, A] = Map(from.map(o => (o.name, o)): _*)
        val bothObject: Seq[(A, A)] = List(to.filter(o => fromMap.contains(o.name)).map(o => (o, fromMap.get(o.name).get)): _*);

        val fromNull: Seq[A] = toMap.filterKeys(o => !fromMap.keySet.contains(o)).values.toList //todo Map.excl use insted
        val toNull: Seq[A] = fromMap.filterKeys(o => !toMap.keySet.contains(o)).values.toList //todo Map.excl use insted
        Tuple3(toNull, fromNull, bothObject)
    }
}

object ColumnDiffBuilder {
    import ListDiffMaker._
    def compareColumns(from: ColumnModel, to: ColumnModel): Option[AbstractAlterColumn] = {
        var diff: Seq[ColumnPropertyDiff] = List[ColumnPropertyDiff]()
        if (!(from.comment == to.comment)) diff = diff ++ List(new ColumnPropertyDiff(CommentValue, from.comment, to.comment))
        if (from.isNotNull != to.isNotNull) diff = diff ++ List(new ColumnPropertyDiff(CommentValue, from.isNotNull, to.isNotNull))
        if (from.isAutoIncrement != to.isAutoIncrement) diff = diff ++ List(new ColumnPropertyDiff(AutoIncrementality, from.isAutoIncrement, to.isAutoIncrement))
        if (!(from.dataType == to.dataType)) diff = diff ++ List(new ColumnPropertyDiff(TypeValue, from.dataType, to.dataType))
        if (!(from.defaultValue == to.defaultValue)) diff = diff ++ List(new ColumnPropertyDiff(DefaultValue, from.defaultValue, to.defaultValue))
        if (!(from.name == to.name)) Some(new AlterColumn(from.name, Some(to.name), diff)) else
        if (diff.size > 0) Some(new AlterColumn(from.name, None, diff)) else
        None
    }
    
    def createColumn(column: ColumnModel) = new CreateColumn(column)
    def dropColumn(columnName: String) = new DropColumn(columnName)

}        


object IndexDiffBuilder {
    import ListDiffMaker._
    
    def doDiff(from: Option[IndexModel], to: Option[IndexModel]): Option[AbstractIndexDiff] = {
        if (from.isDefined && !to.isDefined) Some(new DropIndex(from.get.name))
        else if (!from.isDefined && to.isDefined) Some(new CreateIndex(to.get))
        else if (from.isDefined && to.isDefined) { 
            if (from != to) Some(new AlterIndex(from.get.name, to.get))
                else None
        } else None
    }
}        

object PrimaryKeyDiffBuilder {
    import ListDiffMaker._
    def doDiff(from: Option[PrimaryKey], to: Option[PrimaryKey]): Option[AbstractIndexDiff] = {
        if (from.isDefined && !to.isDefined) Some(new DropPrimaryKey())
        else if (!from.isDefined && to.isDefined) Some(new CreatePrimaryKey(to.get))
        else if (from.isDefined && to.isDefined) {
            if (from != to) Some(new AlterPrimaryKey(from.get, to.get))
            else None
        } else None
    }
}


object TableDiffBuilder {
    import ListDiffMaker._

    def compareTables(from: TableModel, to: TableModel): Option[AbstractTableDiff] = {

        val (toColumnIsEmpty, fromColumnIsEmpty, columnsForCompare) = doListDiff[ColumnModel](from.columns, to.columns)

        val dropColumnDiff = toColumnIsEmpty.map(column => ColumnDiffBuilder.dropColumn(column.name))
        val createColumnDiff = fromColumnIsEmpty.map(column => ColumnDiffBuilder.createColumn(column))
        val alterOnlyColumnDiff = columnsForCompare.flatMap(column => ColumnDiffBuilder.compareColumns(column._1, column._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[AbstractIndexDiff] = PrimaryKeyDiffBuilder.doDiff(from.primaryKey, to.primaryKey).toList

        val (toIndexIsEmpty, fromIndexIsEmpty, indexesForCompare) = doListDiff[IndexModel](from.keys, to.keys)

        val dropIndexesDiff = toIndexIsEmpty.flatMap(idx => IndexDiffBuilder.doDiff(Some(idx), None))
        val createIndexesDiff = fromIndexIsEmpty.flatMap(idx => IndexDiffBuilder.doDiff(None, Some(idx)))
        val alterIndexesDiff = indexesForCompare.flatMap(idx => IndexDiffBuilder.doDiff(Some(idx._1), Some(idx._2)))

        val alterIndexDiff = primaryKeyDiff ++ createIndexesDiff ++ dropIndexesDiff ++ alterIndexesDiff

        if (from.name != to.name) Some(new AlterTable(from.name, Some(to.name), alterColumnDiff, alterIndexDiff))
        else if (alterColumnDiff.size > 0 || alterIndexDiff.size > 0) Some(new AlterTable(from.name, None, alterColumnDiff, alterIndexDiff))
        else None
    }
        
    def doDiff(from: Option[TableModel], to: Option[TableModel]): Option[AbstractTableDiff] = {
        if (from.isDefined && !to.isDefined) Some(new DropTable(from.get.name))
        else if (!from.isDefined && to.isDefined) Some(new CreateTable(to.get))
        else if (from.isDefined && to.isDefined) compareTables(from.get, to.get)
        else None
    }
}

object DatabaseDiffMaker {
    import ListDiffMaker._
    
    def doDiff(from: DatabaseModel, to: DatabaseModel): DatabaseDiff = {
        val (toIsEmpty, fromIsEmpty, tablesForCompare) = doListDiff[TableModel](from.declarations, to.declarations)
        val dropTables = toIsEmpty.map(tbl => new DropTable(tbl.name))
        val createTables = fromIsEmpty.map(tbl => new CreateTable(tbl))
        val alterTable = tablesForCompare.map(tbl => TableDiffBuilder.doDiff(Some(tbl._1), Some(tbl._2)))
        new DatabaseDiff(dropTables ++ createTables ++ alterTable.flatMap(tbl => tbl.toList))
    }
}

// vim: set ts=4 sw=4 et:
