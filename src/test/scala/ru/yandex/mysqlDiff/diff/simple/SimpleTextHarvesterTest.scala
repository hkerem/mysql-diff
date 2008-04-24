package ru.yandex.mysqlDiff.diff.simple;

import scalax.testing._
import ru.yandex.mysqlDiff.model._
import ru.yandex.mysqlDiff.diff.simple._


object SimpleTextHarvesterTest extends TestSuite("Simple SQL script harvester") {
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
        ")";
    var db1 = SimpleTextHarvester.parse(dataBase1);

    assert(db1.name.equals("database"))
    assert(db1.declarations.size == 1)
    val table = db1.declarations(0)
    
    assert(table.name.equals("file_moderation_history_"))
    assert(table.columns.size == 8)
    val cols = table.columns
    
    assert(cols(0).comment == null)
    assert(cols(1).comment == null)
    assert(cols(2).comment == null)
    assert(cols(3).comment == null)
    assert(cols(4).comment != null && cols(4).comment.equals("test comment rename_from: test"))
    assert(cols(5).comment == null)
    assert(cols(6).comment == null)
    assert(cols(7).comment == null)
    
    assert(cols(0).isAutoIncrement)
    assert(!cols(1).isAutoIncrement)
    assert(!cols(2).isAutoIncrement)
    assert(!cols(3).isAutoIncrement)
    assert(!cols(4).isAutoIncrement)
    assert(!cols(5).isAutoIncrement)
    assert(!cols(6).isAutoIncrement)
    assert(!cols(7).isAutoIncrement)
    
    assert(cols(0).name.equals("id"))
    assert(cols(1).name.equals("user_id"))
    assert(cols(2).name.equals("file_id"))
    assert(cols(3).name.equals("moderator_login"))
    assert(cols(4).name.equals("moderation_time"))
    assert(cols(5).name.equals("reason"))
    assert(cols(6).name.equals("resolution"))
    assert(cols(7).name.equals("message"))
    
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
}
