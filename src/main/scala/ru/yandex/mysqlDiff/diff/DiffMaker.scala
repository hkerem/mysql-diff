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
    
    def compareColumns(from: ColumnModel, to: ColumnModel): Option[ColumnDiff] = {
        var diff = List[ColumnPropertyDiff]()
        if (from.comment != to.comment)
            diff += new ColumnPropertyDiff(CommentValue, from.comment, to.comment)
        if (from.isNotNull != to.isNotNull)
            diff += new ColumnPropertyDiff(CommentValue, from.isNotNull, to.isNotNull)
        if (from.isAutoIncrement != to.isAutoIncrement)
            diff += new ColumnPropertyDiff(AutoIncrementality, from.isAutoIncrement, to.isAutoIncrement)
        if (from.dataType != to.dataType)
            diff += new ColumnPropertyDiff(DataTypeValue, from.dataType, to.dataType)
        if (from.defaultValue != to.defaultValue)
            diff += new ColumnPropertyDiff(DefaultValue, from.defaultValue, to.defaultValue)
        
        if (from.name != to.name) Some(new ChangeColumnDiff(from.name, Some(to.name), diff))
        else if (diff.size > 0) Some(new ChangeColumnDiff(from.name, None, diff))
        else None
    }
    
    def comparePrimaryKeys(fromO: Option[PrimaryKey], toO: Option[PrimaryKey]): Option[IndexDiff] =
        (fromO, toO) match {
            case (Some(from), None) => Some(DropPrimaryKeyDiff(from))
            case (None, Some(to)) => Some(CreatePrimaryKeyDiff(to))
            case (Some(from), Some(to)) => Some(new ChangePrimaryKeyDiff(from, to))
            case (None, None) => None
        }
    
    def compareIndexes(from: IndexModel, to: IndexModel): Option[IndexDiff] = {
        if (from != to) Some(new ChangeIndexDiff(from, to))
        else None
    }
    
    def compareTables(from: TableModel, to: TableModel): Option[TableDiff] = {

        val (fromColumns, toColumns, changeColumnPairs) = compareSeqs(from.columns, to.columns, (x: ColumnModel, y: ColumnModel) => x.name == y.name)

        val dropColumnDiff = fromColumns.map(c => DropColumnDiff(c.name))
        val createColumnDiff = toColumns.map(c => CreateColumnDiff(c))
        val alterOnlyColumnDiff = changeColumnPairs.flatMap(c => compareColumns(c._1, c._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[IndexDiff] = comparePrimaryKeys(from.primaryKey, to.primaryKey).toList

        val (fromIndexes, toIndexes, changeIndexPairs) = compareSeqs(from.keys, to.keys, (x: IndexModel, y: IndexModel) => x.name == y.name)

        val dropIndexesDiff = fromIndexes.map(idx => DropIndexDiff(idx))
        val createIndexesDiff = toIndexes.map(idx => CreateIndexDiff(idx))
        val alterIndexesDiff = changeIndexPairs.flatMap(idx => compareIndexes(idx._1, idx._2))

        val alterIndexDiff = primaryKeyDiff ++ createIndexesDiff ++ dropIndexesDiff ++ alterIndexesDiff

        if (from.name != to.name)
            Some(new ChangeTableDiff(from.name, Some(to.name), alterColumnDiff, alterIndexDiff))
        else if (alterColumnDiff.size > 0 || alterIndexDiff.size > 0)
            Some(new ChangeTableDiff(from.name, None, alterColumnDiff, alterIndexDiff))
        else
            None
    }
        
    def compareDatabases(from: DatabaseModel, to: DatabaseModel): DatabaseDiff = {
        val (toIsEmpty, fromIsEmpty, tablesForCompare) = compareSeqs(from.declarations, to.declarations, (x: TableModel, y: TableModel) => x.name == y.name)
        val dropTables = toIsEmpty.map(tbl => new DropTableDiff(tbl.name))
        val createTables = fromIsEmpty.map(tbl => new CreateTableDiff(tbl))
        val alterTable = tablesForCompare.map(tbl => compareTables(tbl._1, tbl._2))
        new DatabaseDiff(dropTables ++ createTables ++ alterTable.flatMap(tbl => tbl.toList))
    }
}        

// vim: set ts=4 sw=4 et:
