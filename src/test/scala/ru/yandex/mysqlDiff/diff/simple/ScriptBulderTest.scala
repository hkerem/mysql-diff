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
 
 "Column change type" is {
   val c1_1 = new ColumnModel("id", new DataType("int", None))
   val c1List = List(c1_1)
   val table1 = new TableModel("table_test", c1List)
   c1_1.parent = table1
   
   val c2_1 = new ColumnModel("id", new DataType("varchar", Some(100)))
   val table2 = new TableModel("table_test", List(c2_1))
   c2_1.parent = table2
   val tMaker = new TableDiffMaker(table1, table2)
   tMaker.doDiff(x => {
     val res = SimpleScriptBuilder.getString(x).replaceAll("[\\n]+", "")
     assert("Alter value is: " + res, "ALTER TABLE table_test MODIFY COLUMN id varchar(100);".equals(res))
     true
   })
 }
 
 
 "Column droped" is {
   val c1_1 = new ColumnModel("id", new DataType("int", None))
   val c1List = List(c1_1)
   val table1 = new TableModel("table_test", c1List)
   c1_1.parent = table1
   
   
   val c2_1 = new ColumnModel("id", new DataType("int", None))
   val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
   val table2 = new TableModel("table_test", List(c2_1, c2_2))
   c2_1.parent = table2
   c2_2.parent = table2
   
   
   val tMaker = new TableDiffMaker(table2, table1)
   
   tMaker.doDiff(x => {
     val res = SimpleScriptBuilder.getString(x).replaceAll("[\\n]+", "")
     assert("Alter value is:" + res, "ALTER TABLE table_test DROP COLUMN name;".equals(res))
     true
   });
   
   
   "Double test" is {
     val c1_1 = new ColumnModel("id", new DataType("int", None))
     val c1List = List(c1_1)
     val table1 = new TableModel("table_test", c1List)
     c1_1.parent = table1
     
     
     val c2_1 = new ColumnModel("id", new DataType("int", Some(12)))
     val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
     val table2 = new TableModel("table_test", List(c2_1, c2_2))
     c2_1.parent = table2
     c2_2.parent = table2
     
     
     val tMaker = new TableDiffMaker(table1, table2)
     
     var typeDiff = false
     var columnAdd = false
     
     tMaker.doDiff(x => {
       val res = SimpleScriptBuilder.getString(x)
       val strings = res.split("\n")
       assert(strings.length == 2)
       assert("ALTER TABLE table_test MODIFY COLUMN id int(12);".equalsIgnoreCase(strings(0)) || "ALTER TABLE table_test MODIFY COLUMN id int(12);".equalsIgnoreCase(strings(1)))
       assert("ALTER TABLE table_test ADD COLUMN name varchar(1000);".equalsIgnoreCase(strings(0)) || "ALTER TABLE table_test ADD COLUMN name varchar(1000);".equalsIgnoreCase(strings(1)))
       assert(!strings(0).equals(strings(1)))
       true
     })
   }
   
 }
 
 "Remove and create table" is {
   val c1_1 = new ColumnModel("id", new DataType("int", None))
   val c1List = List(c1_1)
   val table1 = new TableModel("table_test1", c1List)
   c1_1.parent = table1
   val d1 = new DatabaseModel("base", List(table1))
   
   val c2_1 = new ColumnModel("id", new DataType("int", None))
   val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
   val table2 = new TableModel("table_test", List(c2_1, c2_2))
   c2_1.parent = table2
   c2_2.parent = table2
   val d2 = new DatabaseModel("base", List(table2))
   
   val tList1 = List(table1)
   val tList2 = List(table2)
   val diff = new DatabaseDiffMaker(d1, d2);
   diff.doDiff(x => {
     val diffList = x.asInstanceOf[DatabaseDiff[SqlObjectType, DiffType[SqlObjectType]]].diffList;
     val res = SimpleScriptBuilder.getString(x)
     assert(diffList.size == 2)
     assert(diffList(0).isInstanceOf[FromIsNull[TableModel]] || diffList(1).isInstanceOf[FromIsNull[TableModel]])
     assert(diffList(0).isInstanceOf[ToIsNull[TableModel]] || diffList(1).isInstanceOf[ToIsNull[TableModel]])
     assert(diffList(0).getClass != diffList(1).getClass)
     /*
        CREATE TABLE table_test (id int, name varchar(1000));DROP TABLE table_test1;       
       */
     val trimmed = res.trim().replaceAll(";[\\s\\n]*", ";").replaceAll(",[\\s\\n]*", ", ")
     //Console.println("\n>>" + trimmed + "<<")
     assert("CREATE TABLE table_test (id int, name varchar(1000));DROP TABLE table_test1;".equals(trimmed))
     true
   })
 }
 
}
