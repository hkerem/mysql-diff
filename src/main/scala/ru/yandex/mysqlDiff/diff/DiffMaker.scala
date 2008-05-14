package ru.yandex.mysqlDiff.diff

import scala.collection.mutable.ArrayBuffer

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
    
    def compareColumns(from: ColumnModel, to: ColumnModel): Option[ChangeColumnDiff] = {
        var diff = new ArrayBuffer[ColumnPropertyDiff]
        
        val comparePropertyTypes = List[ColumnPropertyType](
            CommentPropertyType,
            AutoIncrementPropertyType,
            NullabilityPropertyType,
            DefaultValuePropertyType
        )
        
        if (from.dataType != to.dataType)
            diff += new ChangeColumnPropertyDiff(DataTypeProperty(from.dataType), DataTypeProperty(to.dataType))
        
        for (pt <- comparePropertyTypes) {
            val fromO = from.properties.find(pt)
            val toO = to.properties.find(pt)
            (fromO, toO) match {
                case (Some(fromP), Some(toP)) if fromP != toP => diff += new ChangeColumnPropertyDiff(fromP, toP)
                case (Some(fromP), None) => diff += new DropColumnPropertyDiff(fromP)
                case (None, Some(toP)) => diff += new CreateColumnPropertyDiff(toP)
                case _ =>
            }
        }
        
        if (from.name != to.name) Some(new ChangeColumnDiff(from.name, Some(to.name), diff))
        else if (diff.size > 0) Some(new ChangeColumnDiff(from.name, None, diff))
        else None
    }
    
    def comparePrimaryKeys(fromO: Option[PrimaryKey], toO: Option[PrimaryKey]): Option[IndexDiff] =
        (fromO, toO) match {
            case (Some(from), None) => Some(DropPrimaryKeyDiff(from))
            case (None, Some(to)) => Some(CreatePrimaryKeyDiff(to))
            case (Some(from), Some(to)) if from.columns.toList != to.columns.toList =>
                Some(new ChangePrimaryKeyDiff(from, to))
            case _ => None
        }
    
    def compareIndexes(from: IndexModel, to: IndexModel): Option[IndexDiff] = {
        if (from.columns.toList != to.columns.toList || from.isUnique != to.isUnique) Some(new ChangeIndexDiff(from, to))
        else None
    }
    
    def compareTables(from: TableModel, to: TableModel): Option[TableDiff] = {

        val (fromColumns, toColumns, changeColumnPairs) = compareSeqs(from.columns, to.columns, (x: ColumnModel, y: ColumnModel) => x.name == y.name)

        val dropColumnDiff = fromColumns.map(c => DropColumnDiff(c.name))
        val createColumnDiff = toColumns.map(c => CreateColumnDiff(c))
        val alterOnlyColumnDiff = changeColumnPairs.flatMap(c => compareColumns(c._1, c._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[IndexDiff] = comparePrimaryKeys(from.primaryKey, to.primaryKey).toList

        val (fromIndexes, toIndexes, changeIndexPairs) = compareSeqs(from.indexes, to.indexes, (x: IndexModel, y: IndexModel) => x.name == y.name)

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

object DiffMakerTests extends org.specs.Specification {
    import org.specs.matcher.Matcher
    
    "compareSeqs" in {
        val a = List(1, 2, 3, 5)
        val b = List("4", "3", "2")

        def comparator(x: Int, y: String) = x.toString == y
        val (onlyInA, onlyInB, inBoth) = DiffMaker.compareSeqs(a, b, comparator _)

        List(1, 5) must_== onlyInA.toList
        List("4") must_== onlyInB.toList
        List((2, "2"), (3, "3")) must_== inBoth.toList
    }
    
    "compareColumns rename" in {
        val oldC = new ColumnModel("user", DataType.varchar(10), new ColumnProperties(List(Nullability(true))))
        val newC = new ColumnModel("user_name", DataType.varchar(10), new ColumnProperties(List(Nullability(true))))
        val diff = DiffMaker.compareColumns(oldC, newC).get
        diff must beLike { case ChangeColumnDiff("user", Some("user_name"), Seq()) => true; case _ => false }
    }
    
    // probably too complex
    case class changeProperty(oldValue: ColumnProperty, newValue: ColumnProperty)
        extends Matcher[ChangeColumnDiff]
    {
        require(oldValue.propertyType == newValue.propertyType)
        val propertyType = oldValue.propertyType
        
        override def apply(ccf: => ChangeColumnDiff) = {
            val title = ccf.toString
            val changeO = ccf.changeDiff.find(_.propertyType == propertyType)
            changeO match {
                case None =>
                    (false,
                        title + " has change of property " + propertyType,
                        title + " has no change of property " + propertyType)
                case Some(change @ ChangeColumnPropertyDiff(oldP, newP)) =>
                    val result = (oldValue, newValue) == (oldP, newP)
                    (result,
                        title + " has change to " + (oldValue, newValue),
                        title + " has wrong change " + change + ", should be " + (oldValue, newValue))
            }
        }
    }
    
    def changeDataType(oldType: DataType, newType: DataType) =
        changeProperty(new DataTypeProperty(oldType), new DataTypeProperty(newType))
    
    "compareColumns change type" in {
        val oldC = new ColumnModel("user", DataType.varchar(10), new ColumnProperties(List(Nullability(true))))
        val newC = new ColumnModel("user", DataType.varchar(9), new ColumnProperties(List(Nullability(true))))
        val diff = DiffMaker.compareColumns(oldC, newC).get
        diff must changeDataType(DataType.varchar(10), DataType.varchar(9))
        diff.diff.length must_== 1
    }
} //~

// vim: set ts=4 sw=4 et:
