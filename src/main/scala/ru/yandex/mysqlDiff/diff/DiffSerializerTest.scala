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


        val diff = DiffMaker.compareTables(table1, table2)
        val resultScript = TableScriptBuilder.alterScript(diff.get.asInstanceOf[ChangeTableDiff], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                    case s: ScriptStatement => Some(ScriptSerializer.serialize(s))
                })
        /*
--Modify Table "table_test"
--Create Columns
ALTER TABLE table_test ADD COLUMN name varchar (Some(1000));
--End modify Table "table_test"
        */
        assert(resultScript.size == 1)
        assert("ALTER TABLE table_test ADD COLUMN name VARCHAR(1000) NULL" == resultScript(0).trim)
    }

    "Column change type" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test", c1List)
   
        val c2_1 = new ColumnModel("id", new DataType("varchar", Some(100), false, false, None, None))
        val table2 = new TableModel("table_test", List(c2_1))

        val diff = DiffMaker.compareTables(table1, table2)
        val resultScript = TableScriptBuilder.alterScript(diff.get.asInstanceOf[ChangeTableDiff], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                    case s: ScriptStatement => Some(ScriptSerializer.serialize(s))
                })

        //ALTER TABLE table_test MODIFY COLUMN id varchar(100);

        assert(resultScript.size == 1)

        assert("ALTER TABLE table_test MODIFY COLUMN id varchar(100) NULL" == resultScript(0).trim)
    }
 
    "Column droped" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val c1_2 = new ColumnModel("name", new DataType("varchar", Some(1000), false, false, None, None))
        val table1 = new TableModel("table_test", List(c1_1, c1_2))

        val c2_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val table2 = new TableModel("table_test", List(c2_1))
   
        val diff = DiffMaker.compareTables(table1, table2)
        val resultScript = TableScriptBuilder.alterScript(diff.get.asInstanceOf[ChangeTableDiff], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                    case s: ScriptStatement => Some(ScriptSerializer.serialize(s))
                })

        assert(resultScript.size == 1)
        assert("ALTER TABLE table_test DROP COLUMN name" == resultScript(0).trim)
    }

    "Double test" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val table1 = new TableModel("table_test", List(c1_1))

     
     
        val c2_1 = new ColumnModel("id", new DataType("int", Some(12), false, false, None, None))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000), false, false, None, None))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))


//ALTER TABLE table_test MODIFY COLUMN id int(12);
//ALTER TABLE table_test ADD COLUMN name varchar(1000);
        val diff = DiffMaker.compareTables(table1, table2)
        val resultScript = TableScriptBuilder.alterScript(diff.get.asInstanceOf[ChangeTableDiff], table2)
                .flatMap(e => e match {
                    case e: CommentElement => None
                    case Unparsed(u) => Some(u)
                    case s: ScriptStatement => Some(ScriptSerializer.serialize(s))
                })

        assert(resultScript.size == 2)
        val scriptSet = Set(resultScript: _*)
        val assertSet = Set("ALTER TABLE table_test MODIFY COLUMN id int(12) NULL", "ALTER TABLE table_test ADD COLUMN name varchar(1000) NULL")
        assert(scriptSet == assertSet)
    }


    "Remove and create table" is {
        val c1_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val c1List = List(c1_1)
        val table1 = new TableModel("table_test1", c1List)
        val d1 = new DatabaseModel("base", List(table1))
   
        val c2_1 = new ColumnModel("id", new DataType("int", None, false, false, None, None))
        val c2_2 = new ColumnModel("name", new DataType("varchar", Some(1000), false, false, None, None))
        val table2 = new TableModel("table_test", List(c2_1, c2_2))
        val d2 = new DatabaseModel("base", List(table2))
   
        val diff = DiffMaker.compareDatabases(d1, d2)
        
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
} 
