package ru.yandex.mysqlDiff
package util.getopt

class GetoptException(message: String) extends Exception(message)

case class Opt(name: String, requiresArg: Boolean)

object OptionLexer extends scala.util.parsing.combinator.lexical.StdLexical {
    import scala.util.parsing.input.CharArrayReader.EofCh
    
    trait Arg
    case class Name(name: String) extends Arg
    case class NameValue(name: String, value: String) extends Arg
    case class JustArg(value: String) extends Arg
    case object EndOfOptions extends Arg
    
    private def optionNameChar: Parser[Char] = elem("option name char", x => x.isLetter || x.isDigit)
    
    private def option: Parser[Arg] =
        ( EofCh ^^^ EndOfOptions
        | rep1(optionNameChar) ~ opt('=' ~> rep(chrExcept(EofCh))) ^^ {
            case name ~ None => Name(name.mkString(""))
            case name ~ Some(value) => NameValue(name.mkString(""), value.mkString(""))
        }
        )
    
    def arg: Parser[Arg] =
        ( '-' ~> '-' ~> option
        | rep(chrExcept(EofCh)) ^^ (a => JustArg(a.mkString("")))
        )
    
    def parse(arg: String) = {
        phrase(this.arg)(new scala.util.parsing.input.CharSequenceReader(arg)) match {
            case Success(result, _) => result
            case ns: NoSuccess =>
                throw new GetoptException(ns.toString) // XXX: make singleline
        }
    }
}

case class Options(options: Seq[Opt]) {
    import OptionLexer._
    
    def getOption(name: String) =
        options.find(_.name == name).getOrElse(throw new GetoptException("unknown option: " + name))
    
    def parse(args: Seq[String]): Result = {
        args match {
            case Seq() => Result(Seq(), Seq())
            case Seq(arg, unparsed @ _*) =>
                OptionLexer.parse(arg) match {
                    case JustArg(_) => Result(Seq(), args)
                    case EndOfOptions => Result(Seq(), unparsed)
                    case Name(name) =>
                        val opt = getOption(name)
                        if (opt.requiresArg) {
                            args match {
                                case Seq(_, value, unparsed @ _*) =>
                                    Result(Seq((opt, value)), Seq()) + parse(unparsed)
                                case _ =>
                                    throw new GetoptException("option " + name + " requires arg")
                            }
                        } else
                            Result(Seq((opt, "")), Seq()) + parse(args.drop(1))
                    case NameValue(name, value) =>
                        val opt = getOption(name)
                        if (!opt.requiresArg)
                            throw new GetoptException("option " + name + " does not require arg")
                        Result(Seq((opt, value)), Seq()) + parse(args.drop(1))
                }
        }
    }
}

case class Result(options: Seq[(Opt, String)], rest: Seq[String]) {
    def +(that: Result) =
        Result(this.options ++ that.options, this.rest ++ that.rest)
}

object GetoptTests extends MySpecification {
    "just args" in {
        Options(Seq()).parse(Seq("ar", "bg")) must beLike {
            case Result(Seq(), Seq("ar", "bg")) => true
            case _ => false
        }
    }
    
    "complex" in {
        val args = Seq("--version", "--display=ugu", "ls", "pwd")
        val display = Opt("display", true)
        val version = Opt("version", false)
        Options(Seq(Opt("display", true), Opt("version", false))).parse(args) must beLike {
            case Result(Seq((`version`, ""), (`display`, "ugu")), Seq("ls", "pwd")) => true
            case _ => false
        }
    }
}

// vim: set ts=4 sw=4 et:
