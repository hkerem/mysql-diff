package ru.yandex.mysqlDiff.diff

import scalax.testing._
import ru.yandex.mysqlDiff.model._
import ru.yandex.mysqlDiff.diff._

object ScriptBulderTest extends TestSuite("Simple Diff Script Bulder test") {



    "Column add to table" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test", c1List)

   
        val c2_1 = new ColumnModel("id", new DataType("int", None))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))


        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val script = TableScriptBuilder.getAlterScript(table1, table2, diff.get.asInstanceOf[TableDiffModel])
        /*
--Modify Table "table_test"
--Create Columns
ALTER TABLE table_test ADD COLUMN name varchar (Some(1000));
--End modify Table "table_test"
        */
        assert(script.size == 4)
        assert("ALTER TABLE table_test ADD COLUMN name varchar (1000);".equals(script(2).trim))
    }

    "Column change type" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test", c1List)
   
        val c2_1 = new ColumnModel("id", new DataType("varchar", Some(100)))
        val table2 = new TableModel("table_test", List(c2_1))

        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val script = TableScriptBuilder.getAlterScript(table1, table2, diff.get.asInstanceOf[TableDiffModel])

        //ALTER TABLE table_test MODIFY COLUMN id varchar(100);

        assert(script.size == 4)
        assert("ALTER TABLE table_test MODIFY COLUMN id varchar (100);".equals(script(2).trim))
    }
 
    "Column droped" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table1 = new TableModel("table_test", List(c1_1, c1_2))


        val c2_1 = new ColumnModel("id", new DataType("int", None))
        val table2 = new TableModel("table_test", List(c2_1))
   

        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val script = TableScriptBuilder.getAlterScript(table1, table2, diff.get.asInstanceOf[TableDiffModel])


        assert(script.size == 4)
        assert("ALTER TABLE table_test DROP COLUMN name ;".equals(script(2).trim))
    }
/*
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
     
     
        var typeDiff = false
        var columnAdd = false
        val m: TableDiffMaker.AddDiffFunction = x => {
            val res = ScriptBuilder.getString(x)
            val strings = res.split("\n")
            assert(strings.length == 2)
            assert("ALTER TABLE table_test MODIFY COLUMN id int(12);".equalsIgnoreCase(strings(0)) || "ALTER TABLE table_test MODIFY COLUMN id int(12);".equalsIgnoreCase(strings(1)))
            assert("ALTER TABLE table_test ADD COLUMN name varchar(1000);".equalsIgnoreCase(strings(0)) || "ALTER TABLE table_test ADD COLUMN name varchar(1000);".equalsIgnoreCase(strings(1)))
            assert(!strings(0).equals(strings(1)))
            true
        }
        TableDiffMaker.doDiff(table1, table2, m)
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
        val m: TableDiffMaker.AddDiffFunction = x => {
            val diffList = x.asInstanceOf[DatabaseDiff[SqlObjectType]].diffList;
            val res = ScriptBuilder.getString(x)
            assert(diffList.size == 2)
            assert(diffList(0).isInstanceOf[FromIsNull[_]] || diffList(1).isInstanceOf[FromIsNull[_]])
            assert(diffList(0).isInstanceOf[ToIsNull[_]] || diffList(1).isInstanceOf[ToIsNull[_]])
            assert(diffList(0).getClass != diffList(1).getClass)
       /*
        CREATE TABLE table_test (id int, name varchar(1000));DROP TABLE table_test1;
       */
            val trimmed = res.trim().replaceAll(";[\\s\\n]*", ";").replaceAll(",[\\s\\n]*", ", ")
       //Console.println("\n>>" + trimmed + "<<")
            assert("CREATE TABLE table_test (id int, name varchar(1000));DROP TABLE table_test1;".equals(trimmed))
            true
        }
        DatabaseDiffMaker.doDiff(d1, d2, m);
    }
 
 
    "Alternative for drop" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1_2 = new ColumnModel("name2", new DataType("varchar", Some(1000)))
        val c1_3 = new ColumnModel("name3", new DataType("varchar", Some(1000)))
        val c1List = List(c1_1, c1_2, c1_3)
        val table1 = new TableModel("table_test", c1List)
        c1_1.parent = table1
        c1_2.parent = table1
        c1_3.parent = table1
   
        val c2_1 = new ColumnModel("id", new DataType("int", None))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))
        c2_1.parent = table2
        c2_2.parent = table2
   
   /*
     ALTER TABLE table_test ADD COLUMN name2 varchar(1000);
     ALTER TABLE table_test ADD COLUMN name3 varchar(1000);
     ALTER TABLE table_test DROP COLUMN name;
     --alternative actions for column "table_test.name" from source
     -- ALTER TABLE table_test CHANGE COLUMN name name2 varchar(1000);
     -- ALTER TABLE table_test CHANGE COLUMN name name3 varchar(1000);
   */
   
        val resultOutput = "ALTER TABLE table_test ADD COLUMN name2 varchar(1000);"+
                           "ALTER TABLE table_test ADD COLUMN name3 varchar(1000);" +
                           "ALTER TABLE table_test DROP COLUMN name;" +
                           "--alternative actions for column \"table_test.name\" from source" +
                           "-- ALTER TABLE table_test CHANGE COLUMN name name2 varchar(1000);"+
                           "-- ALTER TABLE table_test CHANGE COLUMN name name3 varchar(1000);"
   
        val m: TableDiffMaker.AddDiffFunction = x => {
            val res = ScriptBuilder.getString(x).trim().replaceAll("[\\s]*[\\n]+[\\s]*", "")
            assert(resultOutput.equals(res))
            true
        }
        TableDiffMaker.doDiff(table2, table1, m)
    }
 
 
 
 
    "Primary keys diff" is {
        val db1Text = "CREATE TABLE table1 (id INT(11) PRIMARY KEY, nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) PRIMARY KEY)"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
   
   //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
 
 
 
    "Primary Index removed" is {
        val db1Text = "CREATE TABLE table1 (id INT(11) PRIMARY KEY, nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP PRIMARY KEY;"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
 
 
    "Columns unique index diff" is {
        val db1Text = "CREATE TABLE table1 (id INT(11) UNIQUE, nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX id;"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
 
 
    "Add index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id, nameId))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 ADD INDEX testName (id, nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }

 
    "Columns add index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id, nameId))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
   
   //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX testName, ADD INDEX testName (id, nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
 
 
    "Add default index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) KEY)"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 ADD INDEX nameId (nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
 
 
    "Index diff" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) KEY)"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) UNIQUE KEY)"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = ScriptBuilder.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX nameId, ADD UNIQUE INDEX nameId (nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m);
    }
    */
}
