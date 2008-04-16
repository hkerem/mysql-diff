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
  
  "List Diff" is {
    
  }
  
  
}
