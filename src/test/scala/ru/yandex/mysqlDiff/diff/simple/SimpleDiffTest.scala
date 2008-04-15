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
  
  
}