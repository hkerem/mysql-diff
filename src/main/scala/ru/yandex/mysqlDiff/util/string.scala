package ru.yandex.mysqlDiff
package util

trait StringImplicits {
    implicit def stringExtras(string: String) = new StringExtras(string)
}

object StringImplicits extends StringImplicits

class StringExtras(string: String) {
    import string._
    
    def % (args: Any*) =
        String.format(string, args.toArray.asInstanceOf[Array[Object]]: _*)
    
    // XXX: add more escapes
    
    def unescapeJava =
        replace("\\n", "\n").replace("\\t", "\t").replace("\\0", "\0")
    
    def escapeJava =
        replace("\n", "\\n").replace("\t", "\\t").replace("\0", "\\0")
}

object StringTests extends org.specs.Specification {
    import StringImplicits._
    
    "%" in {
        "distance between %s and %s is %d km" % ("Moscow", "Kiev", 757) must_== "distance between Moscow and Kiev is 757 km"
    }
    
    "unescapeJava" in {
        "a\\t\\tb".unescapeJava must_== "a\t\tb"
        "a\t\tb".escapeJava must_== "a\\t\\tb"
    }
}

// vim: set ts=4 sw=4 et:
