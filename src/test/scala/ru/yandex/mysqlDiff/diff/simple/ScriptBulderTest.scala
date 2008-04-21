package ru.yandex.mysqlDiff.diff.simple;

import scalax.testing._
import ru.yandex.mysqlDiff.model._
import ru.yandex.mysqlDiff.diff.simple._

object ScriptBulderTest extends TestSuite("Simple Diff Script Bulder test") {
 "Column add to table" is {
   val c1_1 = new ColumnModel("id", new DataType("int", None))
   val c1List = List(c1_1)
   val table1 = new TableModel("table_test", c1List)
   c1_1.parent = table1
   
   
   val c2_1 = new ColumnModel("id", new DataType("int", None))
   val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
   val table2 = new TableModel("table_test", List(c2_1, c2_2))
   c2_1.parent = table2
   c2_2.parent = table2
   
   val tMaker = new TableDiffMaker(table1, table2)
   tMaker.doDiff(x => {
     val res = SimpleScriptBuilder.getString(x).replaceAll("[\\n]+", "")
     assert("Alter value is: " + res, "ALTER TABLE table_test ADD COLUMN name varchar(1000);".equals(res))
     true
   })
 }
 
 
}
