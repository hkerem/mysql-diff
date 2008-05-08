package ru.yandex.mysqlDiff.jdbc

import java.sql._

import model._

import scalax.control.ManagedResource

/*
 * TBD:
 * Extract table engine, default charset
 * Extract keys
 */
object JdbcModelExtractor {
    def extractTable(tableName: String, conn: Connection): TableModel = {
        val data = conn.getMetaData
        val columns = data.getColumns(null, null, tableName, "%")
        var columnsList = List[ColumnModel]()

        while (columns.next) {
            val colName = columns.getString("COLUMN_NAME")
            val colType = columns.getString("TYPE_NAME")
            var colTypeSize: Int = -1
            if (columns.getObject("COLUMN_SIZE") != null) colTypeSize = columns.getInt("COLUMN_SIZE")

            val isNotNull = !columns.getString("IS_NULLABLE").equalsIgnoreCase("yes")
            val isAutoinrement = columns.getString("IS_AUTOINCREMENT").equalsIgnoreCase("YES")

            val isUnsigned = false;
            val isZerofill = false;
            val characterSet: Option[String] = None;
            val collate: Option[String] = None;

            var typeSizeOption: Option[Int] = None
            if (colTypeSize  != -1) typeSizeOption = Some(colTypeSize)

            val cm = new ColumnModel(colName, new DataType(colType, typeSizeOption, isUnsigned, isZerofill, characterSet, collate))
            cm.isNotNull = isNotNull
            cm.isAutoIncrement = isAutoinrement

            columnsList = (columnsList ++ List(cm)).toList
        }
        
        val pk = extractPrimaryKey(tableName, conn)
        
        val t = new TableModel(tableName, columnsList)
        t.primaryKey = pk
        t
    }
    
    private def read[T](rs: ResultSet)(f: ResultSet => T) = {
        var r = List[T]()
        while (rs.next()) {
            r += f(rs)
        }
        r
    }
    
    def extractPrimaryKey(tableName: String, conn: Connection): Option[PrimaryKey] = {
        val rs = conn.getMetaData.getPrimaryKeys(null, null, tableName)
        
        val r = read(rs) { rs =>
            (rs.getString("PK_NAME"), rs.getString("COLUMN_NAME"))
        }
        
        if (r.isEmpty) None
        else {
            val pkName = r.first._1
            
            // check all rows have the same name
            for ((p, _) <- r)
                if (p != pkName)
                    throw new IllegalStateException("got different names for pk: " + p + ", " + pkName)
            
            Some(new PrimaryKey(if (pkName ne null) Some(pkName) else None, r.map(_._2)))
        }
    }

    def parseIndexes(table: TableModel, data: DatabaseMetaData, checkPrimaryKey: Boolean): Seq[IndexModel] = {
        var indexesMap: Map[String, List[String]] = Map()
        val indexes = data.getIndexInfo(null, null, table.name, false, true)
        while (indexes.next) {
            val colName = indexes.getString("COLUMN_NAME")
            val indexName = indexes.getString("INDEX_NAME")
            if (!checkPrimaryKey || !table.primaryKey.isDefined ||
                table.primaryKey.get.name == null || table.primaryKey.get.name != indexName)

            if (indexesMap.contains(indexName))
                indexesMap(indexName) = (indexesMap(indexName) ++ List(colName)).toList
            else
                indexesMap(indexName) = List(colName)
        }

        val resultList = indexesMap.map(x => new IndexModel(Some(x._1), x._2, false))
        if (table.keys == null)
            table.keys = resultList.toList
        else
            table.keys = (table.keys ++ resultList.toList).toList
        resultList.toList
    }



    def parseIndexes(table: TableModel, data: DatabaseMetaData): Seq[IndexModel] = {
        parseIndexes(table, data, false)
    }


    def parseUnique(table: TableModel, data: DatabaseMetaData): Seq[IndexModel] = {
        parseIndexes(table, data, true)
    }

    def extractTables(conn: Connection): Seq[TableModel] = {
        val data: DatabaseMetaData = conn.getMetaData

        val tables: ResultSet = data.getTables(null, "%", "%", List("TABLE").toArray)

        var returnTables = List[TableModel]()

        while (tables.next) {
            val tableName = tables.getString("TABLE_NAME")
            val tableModel = extractTable(tableName, conn)
            val indexes = parseIndexes(tableModel, data)
            val unique = parseUnique(tableModel, data)
            returnTables = (returnTables ++ List(tableModel)).toList
        }
        returnTables
    }
    
    def extractTables(conn: ManagedResource[Connection]): Seq[TableModel] = for (c <- conn) yield extractTables(c)

    def search(url: String): Seq[TableModel] = {
        Class.forName("com.mysql.jdbc.Driver")

        extractTables(ManagedResource(DriverManager.getConnection(url)))
    }


    def parse(connectionString: String): DatabaseModel = new DatabaseModel("database", search(connectionString))
    
    def main(args: scala.Array[String]) {
    	require(args.length == 1)
    	
    	val model = parse(args.first)
    	
    	print(ModelSerializer.serializeDatabaseToText(model))
    }
}

import scalax.testing._
object JdbcModelExtractorTests extends TestSuite("JdbcModelExtractor") {
    Class.forName("com.mysql.jdbc.Driver")
        
    val testDsUrl = "jdbc:mysql://fastshot:3306/mysql_diff_test"
    val testDsUser = "test"
    val testDsPassword = "test"
    
    val conn = ManagedResource(DriverManager.getConnection(testDsUrl, testDsUser, testDsPassword))
    
    private def execute(q: String) {
        for (c <- conn) {
            c.createStatement().execute(q)
        }
    }
    
    private def dropTable(tableName: String) {
        execute("DROP TABLE IF EXISTS " + tableName)
    }
    
    "Simple Table" is {
        dropTable("bananas")
        execute("CREATE TABLE bananas (id INT, color VARCHAR(100), PRIMARY KEY(id))")
        
        val table = for (c <- conn) yield JdbcModelExtractor.extractTable("bananas", c)
        
        assert("bananas" == table.name)
        
        assert("id" == table.columns(0).name)
        assert("INT" == table.columns(0).dataType.name)
        
        assert("color" == table.columns(1).name)
        assert("VARCHAR" == table.columns(1).dataType.name)
        assert(100 == table.columns(1).dataType.length.get)
        
        assert(List("id") == table.primaryKey.get.columns)
    }
}

// vim: set ts=4 sw=4 et:
