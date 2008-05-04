package ru.yandex.mysqlDiff.script

import scalax.testing._
import model._
import diff._


object ScriptParserTest extends TestSuite("Simple SQL script parser") {

    "Simple table" is {
        val dataBase1 = "CREATE TABLE IF NOT EXISTS file_moderation_history_ (\n" +
            "    id INT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
            "    user_id BIGINT NOT NULL,\n" +
            "    file_id INT UNSIGNED ZEROFILL NOT NULL,\n" +
            "    moderator_login VARCHAR(20) NOT NULL,\n" +
            "    moderation_time TIMESTAMP NOT NULL DEFAULT NOW() comment 'test comment rename_from: test',\n" +
            "    reason INT NOT NULL,\n" +
            "    resolution INT NOT NULL,\n" +
            "    message VARCHAR(8192) DEFAULT '',\n" +
            "    PRIMARY KEY (id, user_id, file_id, moderator_login)\n" +
            ")"
        var db1 = ScriptParser.parseModel(dataBase1)
        assert(db1.name.equals("database"))
        assert(db1.declarations.size == 1)
        val table = db1.declarations(0)
        assert(table.name.equals("file_moderation_history_"))
    
        assert(table.primaryKey.isDefined)
        assert(table.primaryKey.get.columns.size == 4)
        val pCols = table.primaryKey.get.columns
        //id, user_id, file_id, moderator_login
        assert("id" ==pCols(0))
        assert("user_id" == pCols(1))
        assert("file_id" == pCols(2))
        assert("moderator_login" == pCols(3))
    
    
        assert(table.columns.size == 8)
        val cols = table.columns
    
        assert(cols(4).comment == Some("test comment rename_from: test"))
    
        assert(cols(0).isAutoIncrement)
        assert(!cols(1).isAutoIncrement)
        assert(!cols(2).isAutoIncrement)
        assert(!cols(3).isAutoIncrement)
        assert(!cols(4).isAutoIncrement)
        assert(!cols(5).isAutoIncrement)
        assert(!cols(6).isAutoIncrement)
        assert(!cols(7).isAutoIncrement)
    
        assert("id" == cols(0).name)
        assert("user_id" == cols(1).name)
        assert("file_id" == cols(2).name)
        assert("moderator_login" == cols(3).name)
        assert("moderation_time" == cols(4).name)
        assert("reason" == cols(5).name)
        assert("resolution" == cols(6).name)
        assert("message" == cols(7).name)
    
        assert(cols(0).isNotNull)
        assert(cols(1).isNotNull)
        assert(cols(2).isNotNull)
        assert(cols(3).isNotNull)
        assert(cols(4).isNotNull)
        assert(cols(5).isNotNull)
        assert(cols(6).isNotNull)
        assert(!cols(7).isNotNull)

        assert(cols(0).dataType.name.equalsIgnoreCase("int"))
        assert(cols(1).dataType.name.equalsIgnoreCase("bigint"))
        assert(cols(2).dataType.name.equalsIgnoreCase("int"))
        assert(cols(3).dataType.name.equalsIgnoreCase("varchar"))
        assert(cols(4).dataType.name.equalsIgnoreCase("timestamp"))
        assert(cols(5).dataType.name.equalsIgnoreCase("int"))
        assert(cols(6).dataType.name.equalsIgnoreCase("int"))
        assert(cols(7).dataType.name.equalsIgnoreCase("varchar"))

    
        assert(cols(0).dataType.isUnsigned)
        assert(!cols(1).dataType.isUnsigned)
        assert(cols(2).dataType.isUnsigned)
        assert(!cols(3).dataType.isUnsigned)
        assert(!cols(4).dataType.isUnsigned)
        assert(!cols(5).dataType.isUnsigned)
        assert(!cols(6).dataType.isUnsigned)
        assert(!cols(7).dataType.isUnsigned)

        assert(!cols(0).dataType.isZerofill)
        assert(!cols(1).dataType.isZerofill)
        assert(cols(2).dataType.isZerofill)
        assert(!cols(3).dataType.isZerofill)
        assert(!cols(4).dataType.isZerofill)
        assert(!cols(5).dataType.isZerofill)
        assert(!cols(6).dataType.isZerofill)
        assert(!cols(7).dataType.isZerofill)
    
    
    
    
        assert(!cols(0).dataType.length.isDefined)
        assert(!cols(1).dataType.length.isDefined)
        assert(!cols(2).dataType.length.isDefined)
        assert(cols(3).dataType.length.isDefined && cols(3).dataType.length.get == 20)
        assert(!cols(4).dataType.length.isDefined)
        assert(!cols(5).dataType.length.isDefined)
        assert(!cols(6).dataType.length.isDefined)
        assert(cols(7).dataType.length.isDefined && cols(7).dataType.length.get == 8192)
    }
  
  
  
    "Primary key parse test" is {
        val db1Text = "create table bla_bla ( " +
            "id integet primary key" +
            ")"
        val db1 = ScriptParser.parseModel(db1Text)
        val table = db1.declarations(0)
        assert(table.name == "bla_bla")
        assert(table.columns.size == 1)
        assert(table.primaryKey.isDefined)
        assert(table.primaryKey.get.columns.size == 1)
        assert(table.primaryKey.get.columns(0) == "id")
    }


    "Some more table create" is {
        val dbText = "Create table bla1 (" +
            "id int,\n" +
            "name varchar(500)\n" +
            ") ENGINE = mysql ; \n" +
            "\n\n\n" +
            "Create table bla2 (" +
            "id2 int,\n" +
            "name varchar(300));"


        val db = ScriptParser.parseModel(dbText)
        val tables = db.declarations

        assert(tables.size == 2)
        assert(tables(0).columns.size == 2)
        assert(tables(1).columns.size == 2)
    }

}
