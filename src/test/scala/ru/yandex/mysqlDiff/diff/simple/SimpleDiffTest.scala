package ru.yandex.mysqlDiff.diff.simple;

import scalax.testing._
import ru.yandex.mysqlDiff.model._


object SimpleDiffTest extends TestSuite("Simple Diff") {
  
  "Name Diff" is {
    
    val o1 = new SqlObjectType("o1")
    val o2 = new SqlObjectType("o2")
    val o1_eq = new SqlObjectType("o1")
    
    val diffEquals = new NameDiffMaker(o1, o1_eq)
    
    var i = 0
    diffEquals.doDiff(o => {
      fail("FAIL ME:" + o)
    })
    assert("Diff is runned", i == 0)
    
    val diffIs = new NameDiffMaker(o1, o2)
    diffIs.doDiff(o => {
      i = i + 1
      assert("First of diff is fail", o.from == o1)
      assert("First of diff is fail", o.to == o2)
      true
    });
  
  }
  
  
  "Column Diff" is {
    val c1 = new ColumnModel("c1", new DataType("int", None));
    val c1_eq = new ColumnModel("c1", new DataType("int", None));
    
    val diffEquals = new ColumnDiffMaker(c1, c1_eq);
    diffEquals.doDiff(o => {
      fail("Fail with:" + o);
    });
    
    val c2 = new ColumnModel("c2", new DataType("varchar", Some(20)));
    val diff = new ColumnDiffMaker(c1, c2);
   
    var isNameFind = false;
    var isDataTypeFind = false;
    var i = 0;
    diff.doDiff(o => {
      if (o.isInstanceOf[NameDiff[ColumnModel]]) isNameFind = true;
      if (o.isInstanceOf[DataTypeDiff[ColumnModel]]) isDataTypeFind = true;
      i = i + 1;
      true
    });
    
    assert("Diff count: " + i, i == 2)
    assert("Is name find", isNameFind)
    assert("Is type find", isDataTypeFind) 
    
  }

  class SimpleListDiff extends NameDiffMaker(null, null) with ListDiffMaker {
    
  }
  
  
  "List Diff" is {
    val c1 = new ColumnModel("c1", new DataType("varchar", Some(11)));
    val c1t = new ColumnModel("c1", new DataType("int", Some(11)));
    val c2 = new ColumnModel("c2", new DataType("varchar", Some(11)));
    val c3 = new ColumnModel("c3", new DataType("int", Some(11)));
    val c4 = new ColumnModel("c4", new DataType("int", Some(11)));
    
    val s1 = List[ColumnModel](c1, c2, c3);
    val s1_eq = List[ColumnModel](c1, c2, c3);
    
    val eqDiff = new SimpleListDiff;
    eqDiff.doListDiff[ColumnModel](
        s1,
        s1_eq,
        (a, b) => {
          assert("A: " + a + "; B:" + b, a == b)
        }
    );
    
    
    val s2 = List[ColumnModel](c2, c3, c4);
    var i = 0;
    
    
    eqDiff.doListDiff[ColumnModel](
        s1,
        s2,
        (a, b) => {
          i = i + 1;
        }
    );
    assert("diff count invalid", i == 4);
  }
  
  
  "Table Diff" is {
    val c1_1 = new ColumnModel("id", new DataType("int", Some(11)));
    val c1_2 = new ColumnModel("name", new DataType("varchar", Some(100)));
    val c1_3 = new ColumnModel("data", new DataType("blob", None));
    
    
    val cList1 = List(c1_1, c1_2, c1_3);
    val table1 = new TableModel("table1", cList1);
    
    
    val c2_1 = new ColumnModel("id", new DataType("int", Some(20)));
    val c2_2 = new ColumnModel("user_name", new DataType("varchar", Some(100)));
    val c2_3 = new ColumnModel("data", new DataType("int", None));
    
    val cList2 = List(c2_1, c2_2, c2_3);
    val table2 = new TableModel("table1", cList2);
    
    
    val tableDiff = new TableDiffMaker(table1, table2);
    
    var tableDiffObject: DiffType[SqlObjectType] = null;
    var i = 0;
    tableDiff.doDiff(
        o => {
          tableDiffObject = o;
          i = i + 1;
          true
        }
    );
    assert(i == 1)
    assert(tableDiffObject != null)
    assert(tableDiffObject.asInstanceOf[TableDiff[SqlObjectType, DiffType[SqlObjectType]]].diffList.size == 4)
  }
  
  
}
