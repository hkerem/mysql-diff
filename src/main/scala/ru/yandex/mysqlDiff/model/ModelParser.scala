package ru.yandex.mysqlDiff.model

import scala.collection.mutable.ArrayBuffer

import Implicits._

import script._

import scalax.io._

/**
 * Parse script into model.
 */
case class ModelParser(val context: Context) {
    import context._
    
    import TableDdlStatement._
    
    def parseModel(text: String): DatabaseModel =
        parseModel(parser.parse(text))
    
    def parseModel(script: Script): DatabaseModel = {
        script.ddlStatements.foldLeft(new DatabaseModel(Nil))((db, stmt) => parseScriptElement(stmt, db))
    }
    
    def parseScriptElement(stmt: DdlStatement, db: DatabaseModel) = stmt match {
        // XXX: handle IF NOT EXISTS
        case ct: CreateTableStatement => db.createTable(parseCreateTable(ct))
        case CreateTableLikeStatement(name, _, like) => db.createTable(db.table(like).withName(name))
        case DropTableStatement(name, _) => db.dropTable(name)
        case st @ AlterTableStatement(name, _) => db.alterTable(name, alterTable(st, _))
    }
    
    private def parseColumn(c: Column) = {
        val Column(name, dataType, attrs) = c
        if (dataType.nameOption == Some("TIMESTAMP") && c.modelProperties.defaultValue.isEmpty)
            // because of MySQL-specifc features that are hard to deal with
            throw new Exception(
                    "TIMESTAMP without DEFAULT value is prohibited, column " + name) // XXX: report table
        new ColumnModel(name, dataType, c.modelProperties)
    }
    
    protected def parseCreateTableExtra(e: Entry) = Seq(e match {
        case Index(index) => index
        case PrimaryKey(pk) => pk
        case ForeignKey(fk) => fk
        case UniqueKey(uk) => uk
    })
    
    def parseCreateTable(ct: CreateTableStatement): TableModel = {
        
        val name = ct.name
        val columns = new ArrayBuffer[ColumnModel]
        val extras = new ArrayBuffer[TableExtra]
        ct.entries.map {
            case column @ Column(name, dataType, attrs) =>
                
                columns += parseColumn(column)
                
                attrs foreach {
                    case InlinePrimaryKey =>
                        extras += PrimaryKeyModel(None, Seq(column.name))
                    
                    case InlineReferences(References(table, Seq(tColumn), updatePolicy, deletePolicy)) =>
                        extras += ForeignKeyModel(None, Seq(column.name), table, Seq(tColumn),
                                updatePolicy, deletePolicy)
                        extras += IndexModel(None, Seq(column.name))
                    
                    // XXX: other inline properties
                    
                    case ModelColumnProperty(_) =>
                }
                
            case e => extras ++= parseCreateTableExtra(e)
        }
        
        val pks = extras.flatMap { case pk: PrimaryKeyModel => Some(pk); case _ => None }
        require(pks.length <= 1)
        
        val pk = pks.firstOption
        
        val columns2 = columns.map {
            c =>
                // XXX: drop here, reasonable only for PK columns
                val defaultAutoincrement = AutoIncrement(false)
                
                val properties = c.properties
                    .withDefaultProperty(defaultAutoincrement)
                
                ColumnModel(c.name, c.dataType, properties)
        }
        
        fixTable(TableModel(name, columns2.toList, extras, ct.options))
    }
    
    protected def fixTable(table: TableModel) = {
        val TableModel(name, columns, extras, options) = table
        TableModel(name, columns.map(fixColumn(_, table)), extras, options)
    }
    
    protected def fixColumn(column: ColumnModel, table: TableModel) =
        column.withDataType(fixDataType(column.dataType, column, table)) match {
            case c if table.isPk(c.name) => fixPkColumn(c)
            case c => fixRegularColumn(c)
        }
    
    protected def fixPkColumn(column: ColumnModel) = {
        var properties = column.properties
        
        // don't know which is better
        properties = properties.overrideProperty(Nullability(false))
        //properties = properties.removeProperty(NullabilityPropertyType)
        
        if (properties.defaultValue == Some(NullValue))
            properties = properties.removeProperty(DefaultValuePropertyType)
        ColumnModel(column.name, column.dataType, properties)
    }
    
    protected def fixRegularColumn(column: ColumnModel) = {
        var properties = column.properties
        properties = properties.withDefaultProperty(Nullability(true))
        properties = properties.withDefaultProperty(DefaultValue(NullValue))
        ColumnModel(column.name, column.dataType, properties)
    }
    
    protected def fixDataType(dataType: DataType, column: ColumnModel, table: TableModel): DataType =
        dataType
    
    def parseCreateTableScript(text: String) =
        parseCreateTable(sqlParserCombinator.parseCreateTableRegular(text))
    
    private def alterTableOperation(op: Operation, table: TableModel): TableModel = {
        import AlterTableStatement._
        op match {
            case AddEntry(c: Column) => table.addColumn(parseColumn(c))
            case AddEntry(Index(i)) => table.addExtra(i)
            case AddEntry(PrimaryKey(pk)) => table.addExtra(pk)
            case AddEntry(ForeignKey(fk)) => table.addExtra(fk)
            case AddEntry(UniqueKey(uk)) => table.addExtra(uk)
            
            case ChangeColumn(oldName, model) => table.alterColumn(oldName, ignored => model)
            case ModifyColumn(model) => table.alterColumn(model.name, ignored => model)
            case DropColumn(name) => table.dropColumn(name)
            case DropPrimaryKey => table.dropPrimaryKey
            case DropIndex(name) => table.dropIndex(name)
            /*
            case DropForeignKey(name) =>
            case AddForeignKey(fk) => 
            */
            case ChangeTableOption(option) => table.overrideOptions(Seq(option))
        }
    }
    
    private def alterTable(stmt: AlterTableStatement, table: TableModel): TableModel = {
        val AlterTableStatement(name, ops) = stmt
        require(name == table.name)
        ops.foldLeft(table)((table, op) => alterTableOperation(op, table))
    }
    
    def main(args: Array[String]) {
        val text = InputStreamResource.file(args(0)).reader.slurp()
        val model = parseModel(text)
        print(modelSerializer.serializeDatabaseToText(model))
    }
}

class ModelParserTests(context: Context) extends org.specs.Specification {
    import context._
    import modelParser._
    
    "unspecified nullability means nullable" in {
        val ctc = TableDdlStatement.Column("age", dataTypes.int,
            new ColumnProperties(List(DefaultValue(NumberValue(0)))))
        val ct = CreateTableStatement("x", false, List(ctc), Nil)
        val t = parseCreateTable(ct)
        val tc = t.column("age")
        tc.properties.find(NullabilityPropertyType) must_== Some(Nullability(true))
    }
    
    "unspecified DEFAULT VALUE means NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (login VARCHAR(10))")
        val c = t.column("login")
        c.properties.defaultValue must_== Some(NullValue)
    }
    
    "unspecified autoincrement" in {
        val t = parseCreateTableScript("CREATE TABLE user (id INT, login VARCHAR(10), PRIMARY KEY(id))")
        t.column("id").properties.autoIncrement must_== Some(false)
        //t.column("login").properties.autoIncrement must_== None
    }
    
    "PK is automatically NOT NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT, name VARCHAR(10), PRIMARY KEY(id))")
        val idColumn = t.column("id")
        idColumn.properties.find(NullabilityPropertyType) must_== Some(Nullability(false))
    }
    
    "inline PK" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT PRIMARY KEY, login VARCHAR(10))")
        t.columns.length must_== 2
        t.primaryKey.get.columns must beLike { case Seq("id") => true; case _ => false }
    }
    
    /*
    "MySQL TIMESTAMP is DEFAULT NOW()" in {
        val ct = sqlParserCombinator.parseCreateTable(
            "CREATE TABLE files (created TIMESTAMP)")
        val t = parseCreateTable(ct)
        val c = t.column("created")
        c.properties.defaultValue must_== Some(NowValue)
    }
    */
    
    "Prohibit TIMESTAMP without DEFAULT value" in {
        val ct = sqlParserCombinator.parseCreateTableRegular(
            "CREATE TABLE x (a TIMESTAMP)")
        try {
            val t = parseCreateTable(ct)
            fail("table should not be allowed, created " + t)
        } catch {
            case e: Exception if e.getMessage contains "prohibited" =>
        }
    }
    
    "DROP TABLE" in {
        val db = modelParser.parseModel("CREATE TABLE a (id INT); CREATE TABLE b (id INT); DROP TABLE a")
        db.tables must beLike { case Seq(TableModel("b", _, _, _)) => true }
    }
    
    "ALTER TABLE" in {
        val db = modelParser.parseModel(
            "CREATE TABLE a (id INT, name VARCHAR(20), password VARCHAR(11)); " +
            "ALTER TABLE a ADD COLUMN login VARCHAR(10); " +
            "ALTER TABLE a CHANGE COLUMN name user_name VARCHAR(20) NOT NULL, DROP COLUMN password")
        val a = db.table("a")
        a.column("id").dataType must beLike { case DefaultDataType("INT", None, _) => true }
        a.column("login").dataType must beLike { case DefaultDataType("VARCHAR", Some(10), _) => true }
        a.column("user_name").dataType must beLike { case DefaultDataType("VARCHAR", Some(20), _) => true }
        a.findColumn("name") must_== None
        a.findColumn("password") must_== None
    }
    
}

object ModelParserTests extends ModelParserTests(Environment.defaultContext)

// vim: set ts=4 sw=4 et:
