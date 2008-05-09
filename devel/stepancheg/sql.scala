import scala.util.parsing.combinator.lexical

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.lexical._
import java.util.regex.{Pattern, Matcher}

import scalax.io._

class SqlLexical extends StdLexical {
    // ?
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    override def letter = elem("letter", x => x.isLetter || x == '_' || x == '%') // ?
    
    //override def token: Parser[Token] =
    //    letter ~ rep(letter | digit)
    
    override def whitespace: Parser[Any] = rep(
        whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ comment
      | ('-' ~ '-' ~ rep(chrExcept(EofCh, '\n')))
    )
    
    protected override def processIdent(name: String) =
        if (reserved contains name.toUpperCase) Keyword(name.toUpperCase) else Identifier(name)
}

object Sql extends StandardTokenParsers {
    override val lexical = new SqlLexical
    
    lexical.delimiters += ("(", ")", "=", ",", ";", "=")
    
    lexical.reserved += ("CREATE", "TABLE", "VIEW", "IF", "NOT", "NULL", "EXISTS",
            "AS", "SELECT", "UNIQUE", "KEY", "INDEX", "PRIMARY", "DEFAULT", "NOW", "AUTO_INCREMENT")
    
    def dataType: Parser[Any] = name ~ opt("(" ~ numericLit ~ ")")
    
    def nullability: Parser[Any] = "NULL" | ("NOT" ~ "NULL")
    
    def sqlValue: Parser[Any] = "NULL" | numericLit | stringLit | ("NOW" ~ "(" ~ ")") // XXX: string constant
    
    def defaultValue: Parser[Any] = "DEFAULT" ~ sqlValue
    
    def column: Parser[Any] = name ~ dataType ~ opt(nullability) ~ opt(defaultValue) ~ opt("AUTO_INCREMENT")
    
    def name: Parser[Any] = ident
    
    def nameList: Parser[Any] = "(" ~ repsep(name, ",") ~ ")"
    
    def trash: Parser[Any] = name | "=" | "DEFAULT"
    
    //def select: Parser[Any] = "SELECT" ~ rep(not(";" | lexical.EOF))
    def select: Parser[Any] = "SELECT" ~ rep(name)
    
    def index: Parser[Any] = opt("UNIQUE") ~ ("INDEX" | "KEY") ~ opt(name) ~ nameList
    
    def pk: Parser[Any] = "PRIMARY" ~ "KEY" ~ nameList
    
    def tableAttr: Parser[Any] = column | index | pk
    
    def ifNotExists: Parser[Any] = "IF" ~ "NOT" ~ "EXISTS"
    
    def createTable: Parser[Any] = "CREATE" ~ "TABLE" ~ opt(ifNotExists) ~ name ~ "(" ~ repsep(tableAttr, ",") ~ ")" ~ rep(trash)
    
    def createView: Parser[Any] = "CREATE" ~ "VIEW" ~ opt(ifNotExists) ~ name ~ opt(nameList) ~ "AS" ~ select
    
    def topLevel: Parser[Any] = createView | createTable
    
    def script: Parser[Any] = repsep(topLevel, ";") <~ opt(";") ~ lexical.EOF
    
    def main(args: Array[String]) {
        val text =
            if (args.length == 1) {
                args(0)
            } else {
                ReaderResource.apply(args(1)).slurp()
            }
        val tokens = new lexical.Scanner(text)
        println(phrase(script)(tokens))
        //println(phrase(script)(tokens).getClass)
        //println(phrase(script)(tokens).get)
        //println(phrase(script)(tokens).get.asInstanceOf[AnyRef].getClass)
    }
}

// vim: set ts=4 sw=4 et:
