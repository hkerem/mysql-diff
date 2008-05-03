package ru.yandex.mysqlDiff.diff


import ru.yandex.mysqlDiff.model._

object ListDiffMaker {
    def doListDiff[A <: SqlObjectType](from: Seq[A], to: Seq[A]): Tuple3[Seq[A], Seq[A], Seq[(A, A)]] = {
        val toMap: scala.collection.Map[String, A] = Map(to.map(o => (o.name, o)): _*)
        val fromMap: scala.collection.Map[String, A] = Map(from.map(o => (o.name, o)): _*)
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
            if (!from.get.equals(to.get)) Some(new AlterIndex(from.get.name, to.get))
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
            if (!from.equals(to)) Some(new AlterPrimaryKey(from.get, to.get))
            else None
        } else None
    }
}


object TableDiffBuilder {
    import ListDiffMaker._

    def compareTables(from: TableModel, to: TableModel): Option[AbstractTableDiff] = {

        val (toColumnIsEmpty, fromColumnIsEmpty, columnsForCompare) = doListDiff[ColumnModel](from.columns, to.columns)

        val dropColumnDiff = toColumnIsEmpty.map(t => ColumnDiffBuilder.dropColumn(t.name))
        val createColumnDiff = fromColumnIsEmpty.map(t => ColumnDiffBuilder.createColumn(t))
        val alterOnlyColumnDiff = columnsForCompare.flatMap(t => ColumnDiffBuilder.compareColumns(t._1, t._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[AbstractIndexDiff] = PrimaryKeyDiffBuilder.doDiff(from.primaryKey, to.primaryKey).toList

        val (toIndexIsEmpty, fromIndexIsEmpty, indexesForCompare) = doListDiff[IndexModel](from.keys, to.keys)

        val dropIndexesDiff = toIndexIsEmpty.flatMap(t => IndexDiffBuilder.doDiff(Some(t), None))
        val createIndexesDiff = fromIndexIsEmpty.flatMap(t => IndexDiffBuilder.doDiff(None, Some(t)))
        val alterIndexesDiff = indexesForCompare.flatMap(t => IndexDiffBuilder.doDiff(Some(t._1), Some(t._2)))

        val alterIndexDiff = primaryKeyDiff ++ createIndexesDiff ++ dropIndexesDiff ++ alterIndexesDiff

        if (from.name != to.name) Some(new TableDiffModel(from.name, Some(to.name), alterColumnDiff, alterIndexDiff))
        else if (alterColumnDiff.size > 0 || alterIndexDiff.size > 0) Some(new TableDiffModel(from.name, None, alterColumnDiff, alterIndexDiff))
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
        val dropTables = toIsEmpty.map(t => new DropTable(t.name)) 
        val createTables = fromIsEmpty.map(t => new CreateTable(t))
        val alterTable = tablesForCompare.map(t => TableDiffBuilder.doDiff(Some(t._1), Some(t._2)))
        new DatabaseDiff(dropTables ++ createTables ++ alterTable.flatMap(t => t.toList))
    }
}

// vim: set ts=4 sw=4 et:
