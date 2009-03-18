package ru.yandex.mysqlDiff.diff

import scala.collection.mutable.ArrayBuffer

import model._

import util.CollectionUtils._

import Implicits._

/**
 * Tool that compares two database models.
 */
case class DiffMaker(val context: Context) {
    import context._

    
    /**
     * Are the values equivalent from DB point of view. For example, 1 and '1' are equivalent.
     */
    def defaultValuesEquivalent(a: SqlValue, b: SqlValue) = {
        def e1(a: SqlValue, b: SqlValue) = (a, b) match {
            case (NumberValue(x), StringValue(y)) if x.toString == y => true
            // XXX: should only for date and time types
            case (StringValue("0000-00-00 00:00:00"), StringValue("0000-00-00")) => true
            case _ => false
        }

        a == b || e1(a, b) || e1(b, a)
    }
    
    def dataTypesEquivalent(a: DataType, b: DataType) =
        dataTypes.equivalent(a, b)
    
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
                
                // don't know how to deal with default values properly
                //case (None, Some(toP)) if pt == DefaultValuePropertyType =>
                //    diff += new ChangeColumnPropertyDiff(DefaultValue(NullValue), toP)
                
                case _ =>
            }
        }
        
        if (from.name != to.name) Some(new ChangeColumnDiff(from.name, Some(to.name), diff))
        else if (diff.size > 0) Some(new ChangeColumnDiff(from.name, None, diff))
        else None
    }
    
    def comparePrimaryKeys(fromO: Option[PrimaryKeyModel], toO: Option[PrimaryKeyModel]): Option[ExtraDiff] =
        (fromO, toO) match {
            case (Some(from), None) => Some(DropExtraDiff(from))
            case (None, Some(to)) => Some(CreateExtraDiff(to))
            case (Some(from), Some(to)) if from.columns.toList != to.columns.toList =>
                Some(new ChangeExtraDiff(from, to))
            case _ => None
        }
    
    /** Are indexes equivalent? */
    def indexesEquivalent(a: IndexModel, b: IndexModel) =
        (a.columns.toList == b.columns.toList) &&
            (a.name == b.name || a.name == None || b.name == None)
    
    /** Are foreign keys equivalent? */
    def fksEquivalent(a: ForeignKeyModel, b: ForeignKeyModel) = {
        val (_, _, ab) =
                compareSeqs(a.properties, b.properties, (a: (Any, Any), b: (Any, Any)) => a._1 == b._1)
        ab.forall { case (a, b) => a == b }
    }
    
    def uksEquivalent(a: UniqueKeyModel, b: UniqueKeyModel) =
        (a.columns.toList == b.columns.toList) &&
            (a.name == b.name || a.name == None || b.name == None)
    
    def pksEquivalent(a: PrimaryKeyModel, b: PrimaryKeyModel) =
        (a.columns.toList == b.columns.toList) &&
            (a.name == b.name || a.name == None || b.name == None)
    
    def extrasEquivalent(a: TableExtra, b: TableExtra) = (a, b) match {
        case (a: IndexModel, b: IndexModel) => indexesEquivalent(a, b)
        case (a: PrimaryKeyModel, b: PrimaryKeyModel) => pksEquivalent(a, b)
        case (a: ForeignKeyModel, b: ForeignKeyModel) => fksEquivalent(a, b)
        case (a: UniqueKeyModel, b: UniqueKeyModel) => uksEquivalent(a, b)
        case (_: IndexModel, _) => false
        case (_: PrimaryKeyModel, _) => false
        case (_: ForeignKeyModel, _) => false
        case (_: UniqueKeyModel, _) => false
    }
        
    def compareTableOptions(a: TableOption, b: TableOption) = {
        if (a == b) None
        else Some(ChangeTableOptionDiff(a, b))
    }
    
    def compareTables(from: TableModel, to: TableModel): Option[ChangeTableDiff] = {

        val (fromColumns, toColumns, changeColumnPairs) =
            compareSeqs(from.columns, to.columns, (x: ColumnModel, y: ColumnModel) => x.name == y.name)

        val dropColumnDiff = fromColumns.map(c => DropColumnDiff(c.name))
        val createColumnDiff = toColumns.map(c => CreateColumnDiff(c))
        val alterOnlyColumnDiff = changeColumnPairs.flatMap(c => compareColumns(c._1, c._2))

        val alterColumnDiff = dropColumnDiff ++ createColumnDiff ++ alterOnlyColumnDiff

        
        val (fromExtras, toExtras, _) = // XXX: check third argument
            compareSeqs(from.extras, to.extras, extrasEquivalent _)

        val dropExtrasDiff = fromExtras.map(k => DropExtraDiff(k))
        val createExtrasDiff = toExtras.map(k => CreateExtraDiff(k))
        val changeExtrasDiff = Nil // we have no alter index
        
        // workaround MySQL whose FOREIGN KEYs must have explicit INDEX
        val recreateFksDiff = {
            // XXX: write findCommonElements method
            val keptIns = compareSeqs(from.indexes, to.indexes, indexesEquivalent _)._3.map(_._1)
            val keptUks = compareSeqs(from.uniqueKeys, to.uniqueKeys, uksEquivalent _)._3.map(_._1)
            val keptPks = compareSeqs(from.primaryKey.toList, to.primaryKey.toList, pksEquivalent _)._3.map(_._1)
            
            val keptIndexColumns = keptIns.map(_.columns) ++ keptUks.map(_.columns) ++ keptPks.map(_.columns)
            
            val keptFks = compareSeqs(from.foreignKeys, to.foreignKeys, fksEquivalent _)._3.map(_._1)
            
            val recreateFks = keptFks.filter(
                fk => !keptIndexColumns.exists(
                    c => c.take(fk.localColumns.length).toList == fk.localColumns.toList))
            
            recreateFks.flatMap(fk => Seq(DropExtraDiff(fk), CreateExtraDiff(fk)))
        }

        val alterExtrasDiff = dropExtrasDiff ++ createExtrasDiff ++ changeExtrasDiff ++ recreateFksDiff
        
        
        val (fromOptions, toOptions, changeOptionPairs) =
            compareSeqs(from.options.properties, to.options.properties,
                    (x: TableOption, y: TableOption) => x.propertyType == y.propertyType)
        
        val dropOptionDiff = Nil: Seq[TableOptionDiff] // cannot drop options
        val createOptionDiff = Nil: Seq[TableOptionDiff] // unknown: do not force creation
        val alterOptionDiff = changeOptionPairs.flatMap(c => compareTableOptions(c._1, c._2))
        
        val alterTableOptionDiff = dropOptionDiff ++ createOptionDiff ++ alterOptionDiff
        
        if (from.name != to.name)
            Some(new ChangeTableDiff(from.name, Some(to.name), alterColumnDiff, alterExtrasDiff, alterTableOptionDiff))
        else if (alterColumnDiff.size > 0 || alterExtrasDiff.size > 0 || alterTableOptionDiff.size > 0)
            Some(new ChangeTableDiff(from.name, None, alterColumnDiff, alterExtrasDiff, alterTableOptionDiff))
        else
            None
    }
    
    def compareTablesScript(from: TableModel, to: TableModel): Seq[script.ScriptElement] =
        compareTables(from, to) match {
            case None => Nil
            case Some(diff) => diff match {
                case c: CreateTableDiff => diffSerializer.serializeCreateTableDiff(c)
                case d: DropTableDiff => diffSerializer.serializeDropTableDiff(d)
                case d: ChangeTableDiff => diffSerializer.serializeChangeTableDiff(d, to)
            }
        }
    
    def compareTablesFromScript(from: String, to: String) = {
        val fromModel = modelParser.parseCreateTableScript(from)
        val toModel = modelParser.parseCreateTableScript(to)
        compareTables(fromModel, toModel)
    }

    /** Compare two database models */
    def compareDatabases(from: DatabaseModel, to: DatabaseModel): DatabaseDiff = {
        val (toIsEmpty, fromIsEmpty, tablesForCompare) = compareSeqs(from.declarations, to.declarations, (x: TableModel, y: TableModel) => x.name == y.name)
        val dropTables = toIsEmpty.map(tbl => new DropTableDiff(tbl.name))
        val createTables = fromIsEmpty.map(tbl => new CreateTableDiff(tbl))
        val alterTable = tablesForCompare.map(tbl => compareTables(tbl._1, tbl._2))
        new DatabaseDiff(dropTables ++ createTables ++ alterTable.flatMap(tbl => tbl.toList))
    }
}

trait DiffMakerMatchers {
    import org.specs.matcher.Matcher
    
    val context: Context
    
    // Useless
    /*
    case class beSameTableModelAs(ref: TableModel) extends Matcher[TableModel] {
        override def apply(table0: => TableModel) = {
            val table = table0
            val diff1 = diffMaker.compareTables(table, ref)
            val diff2 = diffMaker.compareTables(table, ref)
            (diff1, diff2) match {
                case (None, None) =>
                    (true, table + " has no difference with " + ref, "unreachable")
                case (Some(diff), _) => 
                    (false, "unreachable", table + " is different from " + ref + ", diff is " + diff)
                case (_, Some(diff)) =>
                    (false, "unreachable", "ref" + table + " is different from " + table + ", diff is " + diff)
            }
        }
    }
    */

}

object DiffMakerTests extends org.specs.Specification {
    import Environment.defaultContext._

    import org.specs.matcher.Matcher
    import diffMaker._
    
    import vendor.mysql._
    
    "compareColumns rename" in {
        val oldC = new ColumnModel("user", dataTypes.varchar(10), new ColumnProperties(List(Nullability(true))))
        val newC = new ColumnModel("user_name", dataTypes.varchar(10), new ColumnProperties(List(Nullability(true))))
        val diff = diffMaker.compareColumns(oldC, newC).get
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
        val oldC = new ColumnModel("user", dataTypes.varchar(10), new ColumnProperties(List(Nullability(true))))
        val newC = new ColumnModel("user", dataTypes.varchar(9), new ColumnProperties(List(Nullability(true))))
        val diff = diffMaker.compareColumns(oldC, newC).get
        diff must changeDataType(dataTypes.varchar(10), dataTypes.varchar(9))
        diff.diff.length must_== 1
    }
    
    "compareColumn drop AutoIncrement(false) to none" in {
        // we must not output ALTER because we are unsure that result is not AUTO_INCREMENT
        val oldC = new ColumnModel("user", dataTypes.varchar(10), new ColumnProperties(List(AutoIncrement(false))))
        val newC = new ColumnModel("user", dataTypes.varchar(10), new ColumnProperties(List()))
        diffMaker.compareColumns(oldC, newC) must_== None
    }
    
    "changeColumn bug?" in {
        val oldC = new ColumnModel("vote", dataTypes.make("BIGINT"), new ColumnProperties(Seq(DefaultValue(NullValue))))
        val newC = new ColumnModel("vote", dataTypes.make("BIGINT"), new ColumnProperties(Seq(DefaultValue(NullValue))))
        diffMaker.compareColumns(oldC, newC) must_== None
    }
    
    "ignore index name change to none" in {
        val columns = List(new ColumnModel("id", dataTypes.int), new ColumnModel("b", dataTypes.int))
        val i1 = new IndexModel(Some("my_index"), List("b"))
        val i2 = new IndexModel(None, List("b"))
        val t1 = new TableModel("a", columns, Seq(i1), Nil)
        val t2 = new TableModel("a", columns, Seq(i2), Nil)
        compareTables(t1, t2) must_== None
    }
    
    "BIGINT equivalent to BIGINT(19)" in {
        dataTypesEquivalent(dataTypes.make("BIGINT"), dataTypes.make("BIGINT", Some(19))) must_== true
        dataTypesEquivalent(dataTypes.make("BIGINT", Some(19)), dataTypes.make("BIGINT")) must_== true
    }
    
    "INT not equivalent to BIGINT" in {
        dataTypesEquivalent(dataTypes.make("INT"), dataTypes.make("BIGINT")) must_== false
        dataTypesEquivalent(dataTypes.make("BIGINT"), dataTypes.make("INT")) must_== false
    }
    
    "INT not equivalent to VARCHAR(100)" in {
        dataTypesEquivalent(dataTypes.make("INT"), dataTypes.make("VARCHAR", Some(100))) must beFalse
    }
    
    "VARCHAR(10) not equivalent to VARCHAR(20)" in {
        dataTypesEquivalent(dataTypes.make("VARCHAR", Some(10)), dataTypes.make("VARCHAR", Some(20))) must_== false
    }
    
    "VARCHAR(100) equivalent" in {
        val dt = dataTypes.make("VARCHAR", Some(100))
        dataTypesEquivalent(dt, dt) must beTrue
    }
    
    "TINYINT(1) equivalent to BIT" in {
        dataTypesEquivalent(dataTypes.make("BIT"), dataTypes.make("TINYINT", Some(1))) must beTrue
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
