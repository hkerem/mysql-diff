package ru.yandex.mysqlDiff.diff

import scalax.testing._
import ru.yandex.mysqlDiff.model._


object DiffTest extends TestSuite("Simple Diff") {


    "Column Diff" is {
    
        val c1 = new ColumnModel("c1", new DataType("int", None, false, false, None, None))
        val c1_eq = new ColumnModel("c1", new DataType("int", None, false, false, None, None))

        val nodiff = ColumnDiffBuilder.compareColumns(c1, c1_eq)
        assert(!nodiff.isDefined)
    
        val c2 = ColumnModel("c2", new DataType("varchar", Some(20), false, false, None, None))

        val diff = ColumnDiffBuilder.compareColumns(c1, c2)
        assert(diff.isDefined)
        assert(diff.get.isInstanceOf[AlterColumn])
        val diff1 = diff.get.asInstanceOf[AlterColumn]
        assert(diff1.name == "c1")
        assert(diff1.renameTo == Some("c2"))

        val c3 = ColumnModel("c1", new DataType("int", Some(11), false, false, None, None))
        val diff2O = ColumnDiffBuilder.compareColumns(c1, c3)
        assert(diff2O.isDefined)
        val diff2 = diff2O.get.asInstanceOf[AlterColumn]
        assert(!diff2.renameTo.isDefined)
    }
  
/*
    "List Diff" is {
        val c1: ColumnModel = new ColumnModel("c1", new DataType("varchar", Some(11)))
        val c1t: ColumnModel = new ColumnModel("c1", new DataType("int", Some(11)))
        val c2: ColumnModel = new ColumnModel("c2", new DataType("varchar", Some(11)))
        val c3: ColumnModel = new ColumnModel("c3", new DataType("int", Some(11)))
        val c4: ColumnModel = new ColumnModel("c4", new DataType("int", Some(11)))

        val s1 = List[ColumnModel](c1, c2, c3)
        val s1_eq = List[ColumnModel](c1, c2, c3)

        SimpleListDiff.doListDiff[ColumnModel](
            s1,
            s1_eq,
            (a, b) => {
                assert("A: " + a + "; B:" + b, a == b)
            }
        )
    
        val s2 = List[ColumnModel](c2, c3, c4)
        var i = 0
    
    
        SimpleListDiff.doListDiff[ColumnModel](
            s1,
            s2,
            (a, b) => i = i + 1
        )
        assert("diff count invalid", i == 4)
    }
  
  
    "Auto_increment option for column diff test" is {
        val c1: ColumnModel = new ColumnModel("id", new DataType("int", Some(11)))
        c1.isAutoIncrement = true
        val c2: ColumnModel = new ColumnModel("id", new DataType("int", Some(11)))
        var i = 0

        val m : ColumnDiffMaker.AddDiffFunction = x => {
            assert(x.isInstanceOf[AutoIncrementDiff])
            i = i + 1
            true
        }
        ColumnDiffMaker.doDiff(c1, c2, m)
        assert(i == 1)
    }

  
    "Table Diff" is {

        val c1_1: ColumnModel = new ColumnModel("id", new DataType("int", Some(11)))
        val c1_2: ColumnModel = new ColumnModel("name", new DataType("varchar", Some(100)))
        val c1_3: ColumnModel = new ColumnModel("data", new DataType("blob", None))

        val cList1 = List(c1_1, c1_2, c1_3)
        val table1: TableModel = new TableModel("table1", cList1)

    
        val c2_1: ColumnModel = new ColumnModel("id", new DataType("int", Some(20)))
        val c2_2: ColumnModel = new ColumnModel("user_name", new DataType("varchar", Some(100)))
        val c2_3: ColumnModel = new ColumnModel("data", new DataType("int", None))

        val cList2 = List(c2_1, c2_2, c2_3)
        val table2: TableModel = new TableModel("table1", cList2)
    
    

    
        var tableDiffObject: DiffType = null
        var i = 0

        val m: TableDiffMaker.AddDiffFunction = o => {
                tableDiffObject = o
                i = i + 1
                true
        }
        TableDiffMaker.doDiff(table1, table2, m)
        assert(i == 1)
        assert(tableDiffObject != null)
        assert(tableDiffObject.asInstanceOf[TableDiff].diffList.size == 4)
    }
  
    "Column Create Statement" is {
        val c1 = new ColumnModel("id", new DataType("int", None))
        c1.isNotNull = true
        val st = c1.toCreateStatement
        assert("Now create statement is :" + st, "id int NOT NULL".equals(st))
    }
  
    "Table Create Statement" is {
        val c1: ColumnModel = new ColumnModel("id", new DataType("int", Some(11)))
        val c2: ColumnModel = new ColumnModel("name", new DataType("varchar", Some(100)))
        val c3: ColumnModel = new ColumnModel("size", new DataType("integer", None))
        val cList = List(c1, c2, c3)
        val table = new TableModel("test_table", cList)
        val createStatement = table.toCreateStatement.trim.replaceAll("\\s[\\n\\s]*", " ")
    
    /*
        CREATE TABLE test_table (id int(11), name varchar(100), size integer)
    */
        assert("Now create Statement is: " + createStatement,"CREATE TABLE test_table (id int(11), name varchar(100), size integer);".equals(createStatement))
    }
  
  
    "Table Create statement with PK" is {
        val c1 = new ColumnModel("id", new DataType("int", Some(11)))
        val c2 = new ColumnModel("name", new DataType("varchar", Some(100)))
        val cList = List(c1, c2)
        val table = new TableModel("test_table", cList)
        table.primaryKey = new PrimaryKey("", List(c1.name, c2.name))
        val createStatement = table.toCreateStatement.trim.replaceAll("\\s[\\n\\s]*", " ")
        assert("Now create statement is: " + createStatement, "CREATE TABLE test_table (id int(11), name varchar(100), PRIMARY KEY (id, name));".equals(createStatement))
    }
  
  
    "Primary keys diff" is {
        val c1_1 = new ColumnModel("id", new DataType("int", Some(11)))
        val c1_2 = new ColumnModel("name", new DataType("varchar", Some(100)))
        val cList_1 = List(c1_1, c1_2)
        val table1 = new TableModel("test_table", cList_1)
        table1.primaryKey = new PrimaryKey("PRIMARY", List(c1_1.name, c1_2.name))

    
        val c2_1 = new ColumnModel("id", new DataType("int", Some(11)))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(100)))
        val cList2 = List(c2_1, c2_2)
        val table2 = new TableModel("test_table", cList2)
        table2.primaryKey = new PrimaryKey("PRIMARY", List(c2_1.name))

        val m: TableDiffMaker.AddDiffFunction = x => {
            assert(x.isInstanceOf[TableDiff])
            val diffList = x.asInstanceOf[TableDiff].diffList
            assert(diffList.size == 1)
            assert(diffList(0).isInstanceOf[PrimaryKeyDiff])
            val diff = diffList(0).asInstanceOf[PrimaryKeyDiff]
            assert(diff.from == table1.primaryKey)
            assert(diff.to == table2.primaryKey)
            true
        }
        TableDiffMaker.doDiff(table1, table2, m)
    }

*/    
    
    "compareSeqs" is {
        val a = List(1, 2, 3, 5)
        val b = List("4", "3", "2")
        
        def comparator(x: Int, y: String) = x.toString == y
        val (onlyInA, onlyInB, inBoth) = ListDiffMaker.compareSeqs(a, b, comparator _)
        
        assert(List(1, 5) == onlyInA)
        assert(List("4") == onlyInB)
        assert(List((2, "2"), (3, "3")) == inBoth)
    }
}

// vim: set ts=4 sw=4 et:
