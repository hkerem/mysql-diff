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
    
    def defaultValuesEquivalent(a: SqlValue, b: SqlValue) = {
        def e1(a: SqlValue, b: SqlValue) = (a, b) match {
            case (NumberValue(x), StringValue(y)) if x.toString == y => true
            // XXX: should only for date and time types
            case (StringValue("0000-00-00 00:00:00"), StringValue("0000-00-00")) => true
            case _ => false
        }

        a == b || e1(a, b) || e1(b, a)
    }
    
    def dataTypesEquivalent(a: DataType, b: DataType) = {
        def e1(a: DataType, b: DataType) =
            if (a == DataType("TINYINT", Some(1)) && b == DataType("BIT", None)) true
            else false
        
        if (a == b) true
        else if (e1(a, b)) true
        else if (e1(b, a)) true
        else if (a.name != b.name) false
        else if (a.isAnyNumber) true // ignore size change: XXX: should rather know DB defaults
        else if (a.isAnyDateTime) true // probably
        else a.name == b.name && a.length == b.length // ignoring options for a while; should not ignore if options change
    }
    
    def columnPropertiesEquivalent(a: ColumnProperty, b: ColumnProperty) = {
        require(a.propertyType == b.propertyType)
        val propertyType = a.propertyType
        (a, b) match {
            case (DataTypeProperty(adt), DataTypeProperty(bdt)) => dataTypesEquivalent(adt, bdt)
            case (DefaultValue(adv), DefaultValue(bdv)) => defaultValuesEquivalent(adv, bdv)
            case _ => a == b
        }
    }
    
    def compareColumns(from: ColumnModel, to: ColumnModel): Option[ChangeColumnDiff] = {
        var diff = new ArrayBuffer[ColumnPropertyDiff]
        
        val comparePropertyTypes = List[ColumnPropertyType](
            CommentPropertyType,
            AutoIncrementPropertyType,
            NullabilityPropertyType,
            DefaultValuePropertyType
        )
        
        if (!dataTypesEquivalent(from.dataType, to.dataType))
            diff += new ChangeColumnPropertyDiff(DataTypeProperty(from.dataType), DataTypeProperty(to.dataType))
        
        for (pt <- comparePropertyTypes) {
            val fromO = from.properties.find(pt)
            val toO = to.properties.find(pt)
            (fromO, toO) match {
                case (Some(fromP), Some(toP)) if !columnPropertiesEquivalent(fromP, toP) =>
                    diff += new ChangeColumnPropertyDiff(fromP, toP)
                
                case _ =>
            }
        }
        
        if (from.name != to.name) Some(new ChangeColumnDiff(from.name, Some(to.name), diff))
        else if (diff.size > 0) Some(new ChangeColumnDiff(from.name, None, diff))
        else None
    }
    
    def comparePrimaryKeys(fromO: Option[PrimaryKeyModel], toO: Option[PrimaryKeyModel]): Option[KeyDiff] =
        (fromO, toO) match {
            case (Some(from), None) => Some(DropKeyDiff(from))
            case (None, Some(to)) => Some(CreateKeyDiff(to))
            case (Some(from), Some(to)) if from.columns.toList != to.columns.toList =>
                Some(new ChangeKeyDiff(from, to))
            case _ => None
        }
    
    def indexesEquivalent(a: IndexModel, b: IndexModel) =
        (a.columns.toList == b.columns.toList) && (a.isUnique == b.isUnique) &&
            (a.name == b.name || a.name == None || b.name == None)
    
    def fksEquivalent(a: ForeignKeyModel, b: ForeignKeyModel) =
        (a.localColumns.toList == b.columns.toList) &&
                (a.externalTableName == b.externalTableName) && (a.externalColumns == b.externalColumns) &&
                (a.name == b.name || a.name == None || b.name == None)
    
    def keysEquivalent(a: KeyModel, b: KeyModel) = (a, b) match {
        case (a: IndexModel, b: IndexModel) => indexesEquivalent(a, b)
        case (a: ForeignKeyModel, b: ForeignKeyModel) => fksEquivalent(a, b)
        case _ => false
    }
        
    
    def compareTables(from: TableModel, to: TableModel): Option[ChangeTableDiff] = {

        val (fromColumns, toColumns, changeColumnPairs) = compareSeqs(from.columns, to.columns, (x: ColumnModel, y: ColumnModel) => x.name == y.name)

        val dropColumnDiff = fromColumns.map(c => DropColumnDiff(c.name))
        val createColumnDiff = toColumns.map(c => CreateColumnDiff(c))
        val alterOnlyColumnDiff = changeColumnPairs.flatMap(c => compareColumns(c._1, c._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        val primaryKeyDiff: Seq[KeyDiff] = comparePrimaryKeys(from.primaryKey, to.primaryKey).toList

        val (fromKeys, toKeys, changeIndexPairs) = compareSeqs(from.keys, to.keys, keysEquivalent _)

        val dropKeysDiff = fromKeys.map(k => DropKeyDiff(k))
        val createKeysDiff = toKeys.map(k => CreateKeyDiff(k))
        val alterKeysDiff = Nil // we have no alter index

        val alterKeyDiff = primaryKeyDiff ++ createKeysDiff ++ dropKeysDiff ++ alterKeysDiff

        if (from.name != to.name)
            Some(new ChangeTableDiff(from.name, Some(to.name), alterColumnDiff, alterKeyDiff))
        else if (alterColumnDiff.size > 0 || alterKeyDiff.size > 0)
            Some(new ChangeTableDiff(from.name, None, alterColumnDiff, alterKeyDiff))
        else
            None
    }
    
    def compareTablesFromScript(from: String, to: String) = {
        val fromModel = ModelParser.parseCreateTableScript(from)
        val toModel = ModelParser.parseCreateTableScript(to)
        compareTables(fromModel, toModel)
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
    import DiffMaker._
    
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
    
    "compareColumn drop AutoIncrement(false) to none" in {
        val oldC = new ColumnModel("user", DataType.varchar(10), new ColumnProperties(List(AutoIncrement(false))))
        val newC = new ColumnModel("user", DataType.varchar(10), new ColumnProperties(List()))
        DiffMaker.compareColumns(oldC, newC) must_== None
    }
    
    "ignore index name change" in {
        val columns = List(new ColumnModel("id", DataType.int), new ColumnModel("b", DataType.int))
        val i1 = new IndexModel(Some("my_index"), List("b"), true)
        val i2 = new IndexModel(None, List("b"), true)
        val t1 = new TableModel("a", columns, None, List(i1), Nil)
        val t2 = new TableModel("a", columns, None, List(i2), Nil)
        compareTables(t1, t2) must_== None
    }
    
    "BIGINT equivalent to BIGINT(19)" in {
        dataTypesEquivalent(DataType("BIGINT"), DataType("BIGINT", Some(19))) must_== true
        dataTypesEquivalent(DataType("BIGINT", Some(19)), DataType("BIGINT")) must_== true
    }
    
    "INT not equivalent to BIGINT" in {
        dataTypesEquivalent(DataType("INT"), DataType("BIGINT")) must_== false
        dataTypesEquivalent(DataType("BIGINT"), DataType("INT")) must_== false
    }
    
    "INT not equivalent to VARCHAR(100)" in {
        dataTypesEquivalent(DataType("INT"), DataType("VARCHAR", Some(100))) must beFalse
    }
    
    "VARCHAR(10) not equivalent to VARCHAR(20)" in {
        dataTypesEquivalent(DataType("VARCHAR", Some(10)), DataType("VARCHAR", Some(20))) must_== false
    }
    
    "TINYINT(1) equivalent to BIT" in {
        dataTypesEquivalent(DataType("BIT"), DataType("TINYINT", Some(1))) must beTrue
    }
    
    "0 not equivalent to 1" in {
        defaultValuesEquivalent(NumberValue(0), NumberValue(1)) must beFalse
    }
    
    "0 equivalent to '0'" in {
        defaultValuesEquivalent(NumberValue(0), StringValue("0")) must beTrue
    }
    
    "0000-00-00 equivalent to 0000-00-00 00:00:00" in {
        // XXX: should be only for date types
        defaultValuesEquivalent(StringValue("0000-00-00"), StringValue("0000-00-00 00:00:00")) must beTrue
    }
    
} //~

// vim: set ts=4 sw=4 et:
