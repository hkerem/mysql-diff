package ru.yandex.mysqlDiff.diff

import scalax.testing._
import model._
import diff._
import script._

object DiffSerializerTest extends TestSuite("Simple Diff Script Bulder test") {



    "Column add to table" is {
        val c1_1 = new ColumnModel("id", DataType.int())
        val table1 = new TableModel("table_test", List(c1_1))

   
        val c2_1 = new ColumnModel("id", DataType.int())
        val c2_2 = new ColumnModel("name", DataType.varchar(1000))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))


        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val resultScript = TableScriptBuilder.getAlterScript(diff.get.asInstanceOf[AlterTable], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                })
        /*
--Modify Table "table_test"
--Create Columns
ALTER TABLE table_test ADD COLUMN name varchar (Some(1000));
--End modify Table "table_test"
        */
        assert(resultScript.size == 1)
        assert("ALTER TABLE table_test ADD COLUMN name VARCHAR(1000) NULL".equals(resultScript(0).trim))
    }

    "Column change type" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test", c1List)
   
        val c2_1 = new ColumnModel("id", new DataType("varchar", Some(100)))
        val table2 = new TableModel("table_test", List(c2_1))

        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val resultScript = TableScriptBuilder.getAlterScript(diff.get.asInstanceOf[AlterTable], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                })

        //ALTER TABLE table_test MODIFY COLUMN id varchar(100);

        assert(resultScript.size == 1)

        assert("ALTER TABLE table_test MODIFY COLUMN id varchar(100) NULL".equals(resultScript(0).trim))
    }
 
    "Column droped" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table1 = new TableModel("table_test", List(c1_1, c1_2))

        val c2_1 = new ColumnModel("id", new DataType("int", None))
        val table2 = new TableModel("table_test", List(c2_1))
   
        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val resultScript = TableScriptBuilder.getAlterScript(diff.get.asInstanceOf[AlterTable], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                })

        assert(resultScript.size == 1)
        assert("ALTER TABLE table_test DROP COLUMN name".equals(resultScript(0).trim))
    }

    "Double test" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val table1 = new TableModel("table_test", List(c1_1))

     
     
        val c2_1 = new ColumnModel("id", new DataType("int", Some(12)))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))


//ALTER TABLE table_test MODIFY COLUMN id int(12);
//ALTER TABLE table_test ADD COLUMN name varchar(1000);
        val diff = TableDiffBuilder.doDiff(Some(table1), Some(table2))
        val resultScript = TableScriptBuilder.getAlterScript(diff.get.asInstanceOf[AlterTable], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                })

        assert(resultScript.size == 2)
        val scriptSet = Set(resultScript: _*)
        val assertSet = Set("ALTER TABLE table_test MODIFY COLUMN id int(12) NULL", "ALTER TABLE table_test ADD COLUMN name varchar(1000) NULL")
        assert(scriptSet == assertSet)
    }


    "Remove and create table" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test1", c1List)
        val d1 = new DatabaseModel("base", List(table1))
   
        val c2_1 = new ColumnModel("id", new DataType("int", None))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000)))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))
        val d2 = new DatabaseModel("base", List(table2))
   
        val diff = DatabaseDiffMaker.doDiff(d1, d2)
        
        val script = DiffSerializer.serializeToScript(diff, d1, d2)
        
        assert(2 == script.length)
        
        script(0) match {
            case DropTableStatement("table_test1") =>
            case s => fail("first statement expected to be DROP TABLE table_test1, got " + s)
        }
        
        script(1) match {
            case CreateTableStatement(tableFromScript @ TableModel("table_test", _)) =>
                assert(2 == tableFromScript.columns.length)
                tableFromScript.columns(0) match {
                    case ColumnModel("id", _) =>
                    case _ => fail()
                }

                tableFromScript.columns(1) match {
                    case ColumnModel("name", _) =>
                    case _ => fail()
                }
    
                // XXX: check types
            case s => fail("second statement expected to be CREATE TABLE table_test(...), got " + s)
        }
        
        //assert("DROP TABLE table_test1;CREATE TABLE table_test (id int NULL,name varchar(1000) NULL);" == str)
    }
 
/* 
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
            val res = DiffSerializer.getString(x).trim().replaceAll("[\\s]*[\\n]+[\\s]*", "")
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
            val outputScript = DiffSerializer.getString(x).trim
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
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP PRIMARY KEY;"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }
 
 
    "Columns unique index diff" is {
        val db1Text = "CREATE TABLE table1 (id INT(11) UNIQUE, nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX id;"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }
 
 
    "Add index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id, nameId))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 ADD INDEX testName (id, nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }

 
    "Columns add index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12), INDEX testName (id, nameId))"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
   
   //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX testName, ADD INDEX testName (id, nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }
 
 
    "Add default index" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12))"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) KEY)"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 ADD INDEX nameId (nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }
 
 
    "Index diff" is {
        val db1Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) KEY)"
        val db2Text = "CREATE TABLE table1 (id INT(11), nameId INT(12) UNIQUE KEY)"
        val db1 = TextParser.parse(db1Text)
        val db2 = TextParser.parse(db2Text)
     
     //ALTER TABLE table1 DROP PRIMARY KEY, ADD PRIMARY KEY (nameId);
        val m: TableDiffMaker.AddDiffFunction = x => {
            val outputScript = DiffSerializer.getString(x).trim
            assert(outputScript.equals("ALTER TABLE table1 DROP INDEX nameId, ADD UNIQUE INDEX nameId (nameId);"))
            true
        }
        DatabaseDiffMaker.doDiff(db1, db2, m)
    }
    */
}
