package ru.yandex.mysqlDiff.model

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
        case ct: CreateTableStatement => parseCreateTable(ct, db)
        case DropTableStatement(name, _) => db.dropTable(name)
        case st @ AlterTableStatement(name, _) => db.alterTable(name, alterTable(st, _))
    }
    
    private def parseColumn(c: Column) = {
        val Column(name, dataType, attrs) = c
        new ColumnModel(name, dataType, c.modelProperties)
    }
    
    protected def parseCreateTableExtra(e: TableElement, ct: CreateTableStatement) = Seq(e match {
        case Index(index) => index
        case PrimaryKey(pk) => pk
        case ForeignKey(fk) => fk
        case UniqueKey(uk) => uk
    })
    
    // lite version
    final def parseCreateTable(ct: CreateTableStatement): TableModel = {
        parseCreateTable(ct, new DatabaseModel(Seq())).tables match {
            case Seq(table) => table
        }
    }
    
    def parseCreateTable(ct: CreateTableStatement, db: DatabaseModel): DatabaseModel = {
        val CreateTableStatement(name, ifNotExists, TableElementList(elements), options) = ct
        
        val columns = new ArrayBuffer[ColumnModel]
        val extras = new ArrayBuffer[TableExtra]
        elements.map {
            case LikeClause(t) =>
                columns ++= db.table(t).columns
                extras ++= db.table(t).extras
            
            case column @ Column(name, dataType, attrs) =>
                
                columns += parseColumn(column)
                
                attrs foreach {
                    case InlinePrimaryKey =>
                        extras += PrimaryKeyModel(None, Seq(IndexColumn(column.name)))
                    
                    case InlineReferences(References(table, Seq(tColumn), updateRule, deleteRule)) =>
                        extras += ForeignKeyModel(None, Seq(IndexColumn(column.name)), table, Seq(tColumn),
                                updateRule, deleteRule)
                        extras += IndexModel(None, Seq(IndexColumn(column.name)))
                    
                    // XXX: other inline properties
                    
                    case ModelColumnProperty(_) =>
                }
                
            case e => extras ++= parseCreateTableExtra(e, ct)
        }
        
        val pks = extras.flatMap { case pk: PrimaryKeyModel => Some(pk); case _ => None }
        require(pks.length <= 1)
        
        val pk = pks.firstOption
        
        val columns2 = columns.map {
            c =>
                // XXX: drop here, reasonable only for PK columns
                // XXX: autoincrement is meaningful only for MySQL
                val defaultAutoincrement = vendor.mysql.MysqlAutoIncrement(false)
                
                val properties = c.properties
                    .withDefaultProperty(defaultAutoincrement)
                
                ColumnModel(c.name, c.dataType, properties)
        }
        
        db.createTable(fixTable(TableModel(name, columns2.toList, extras, ct.options)))
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
        //properties = properties.removePropertyByType(NullabilityPropertyType)
        
        if (properties.defaultValue == Some(NullValue))
            properties = properties.removePropertyByType(DefaultValuePropertyType)
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
        parseCreateTable(sqlParserCombinator.parseCreateTable(text))
    
    protected def alterTableOperation(op: Operation, table: TableModel): TableModel = {
        import AlterTableStatement._
        op match {
            // XXX: respect position in AddColumn, ChangeColumn, ModifyColumn
            
            case AddColumn(c, position) => table.addColumn(fixColumn(parseColumn(c), table))
            
            case AddExtra(Index(i)) => table.addExtra(i)
            case AddExtra(PrimaryKey(pk)) => table.addExtra(pk)
            case AddExtra(ForeignKey(fk)) => table.addExtra(fk)
            case AddExtra(UniqueKey(uk)) => table.addExtra(uk)
            
            case ChangeColumn(oldName, model, pos) => table.alterColumn(oldName, ignored => model)
            case ModifyColumn(model, pos) => table.alterColumn(model.name, ignored => model)
            case DropColumn(name) => table.dropColumn(name)
            case DropPrimaryKey => table.dropPrimaryKey
            case DropIndex(name) => table.dropIndexOrUniqueKey(name) // XXX: should drop unique key only on MySQL
            case DropForeignKey(name) => table.dropForeignKey(name)
            /*
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
        val t = parseCreateTableScript("CREATE TABLE x (age INT DEFAULT 0)")
        val tc = t.column("age")
        tc.properties.find(NullabilityPropertyType) must_== Some(Nullability(true))
    }
    
    "unspecified DEFAULT VALUE means NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (login VARCHAR(10))")
        val c = t.column("login")
        c.properties.defaultValue must_== Some(NullValue)
    }
    
    "PK is automatically NOT NULL" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT, name VARCHAR(10), PRIMARY KEY(id))")
        val idColumn = t.column("id")
        idColumn.properties.find(NullabilityPropertyType) must_== Some(Nullability(false))
    }
    
    "inline PK" in {
        val t = parseCreateTableScript("CREATE TABLE users (id INT PRIMARY KEY, login VARCHAR(10))")
        t.columns.length must_== 2
        t.primaryKey.get.columnNames.toList must_== List("id")
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
        a.column("id").dataType.name must_== "INT"
        // XXX: enable
        //a.column("login").dataType must beLike { case DefaultDataType("VARCHAR", Some(10)) => true }
        //a.column("user_name").dataType must beLike { case DefaultDataType("VARCHAR", Some(20)) => true }
        a.findColumn("name") must_== None
        a.findColumn("password") must_== None
    }
    
}

object ModelParserTests extends ModelParserTests(Environment.defaultContext)

// vim: set ts=4 sw=4 et:
