package ru.yandex.mysqlDiff.diff


import model._

object DiffMaker {

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
    
    def compareColumns(from: ColumnModel, to: ColumnModel): Option[AbstractAlterColumn] = {
        var diff = List[ColumnPropertyDiff]()
        if (from.comment != to.comment)
            diff += new ColumnPropertyDiff(CommentValue, from.comment, to.comment)
        if (from.isNotNull != to.isNotNull)
            diff += new ColumnPropertyDiff(CommentValue, from.isNotNull, to.isNotNull)
        if (from.isAutoIncrement != to.isAutoIncrement)
            diff += new ColumnPropertyDiff(AutoIncrementality, from.isAutoIncrement, to.isAutoIncrement)
        if (from.dataType != to.dataType)
            diff += new ColumnPropertyDiff(TypeValue, from.dataType, to.dataType)
        if (from.defaultValue != to.defaultValue)
            diff += new ColumnPropertyDiff(DefaultValue, from.defaultValue, to.defaultValue)
        
        if (from.name != to.name) Some(new AlterColumn(from.name, Some(to.name), diff))
        else if (diff.size > 0) Some(new AlterColumn(from.name, None, diff))
        else None
    }
    
    
    def compareIndexes(from: IndexModel, to: IndexModel): Option[AbstractIndexDiff] = {
        if (from != to) Some(new AlterIndex(from.name, to))
        else None
    }
}        

object PrimaryKeyDiffBuilder {
    import DiffMaker._
    
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
    import DiffMaker._

    def compareTables(from: TableModel, to: TableModel): Option[AbstractTableDiff] = {

        val (fromColumns, toColumns, changeColumnPairs) = compareSeqs(from.columns, to.columns, (x: ColumnModel, y: ColumnModel) => x.name == y.name)

        val dropColumnDiff = fromColumns.map(c => DropColumn(c.name))
        val createColumnDiff = toColumns.map(c => CreateColumn(c))
        val alterOnlyColumnDiff = changeColumnPairs.flatMap(c => compareColumns(c._1, c._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[AbstractIndexDiff] = PrimaryKeyDiffBuilder.doDiff(from.primaryKey, to.primaryKey).toList

        val (fromIndexes, toIndexes, changeIndexPairs) = compareSeqs(from.keys, to.keys, (x: IndexModel, y: IndexModel) => x.name == y.name)

        val dropIndexesDiff = fromIndexes.map(idx => DropIndex(idx.name))
        val createIndexesDiff = toIndexes.map(idx => CreateIndex(idx))
        val alterIndexesDiff = changeIndexPairs.flatMap(idx => compareIndexes(idx._1, idx._2))

        val alterIndexDiff = primaryKeyDiff ++ createIndexesDiff ++ dropIndexesDiff ++ alterIndexesDiff

        if (from.name != to.name)
            Some(new AlterTable(from.name, Some(to.name), alterColumnDiff, alterIndexDiff))
        else if (alterColumnDiff.size > 0 || alterIndexDiff.size > 0)
            Some(new AlterTable(from.name, None, alterColumnDiff, alterIndexDiff))
        else
            None
    }
        
    def doDiff(from: Option[TableModel], to: Option[TableModel]): Option[AbstractTableDiff] = {
        if (from.isDefined && !to.isDefined) Some(new DropTable(from.get.name))
        else if (!from.isDefined && to.isDefined) Some(new CreateTable(to.get))
        else if (from.isDefined && to.isDefined) compareTables(from.get, to.get)
        else None
    }
}

object DatabaseDiffMaker {
    import DiffMaker._
    
    def doDiff(from: DatabaseModel, to: DatabaseModel): DatabaseDiff = {
        val (toIsEmpty, fromIsEmpty, tablesForCompare) = compareSeqs(from.declarations, to.declarations, (x: TableModel, y: TableModel) => x.name == y.name)
        val dropTables = toIsEmpty.map(tbl => new DropTable(tbl.name))
        val createTables = fromIsEmpty.map(tbl => new CreateTable(tbl))
        val alterTable = tablesForCompare.map(tbl => TableDiffBuilder.doDiff(Some(tbl._1), Some(tbl._2)))
        new DatabaseDiff(dropTables ++ createTables ++ alterTable.flatMap(tbl => tbl.toList))
    }
}

// vim: set ts=4 sw=4 et:
