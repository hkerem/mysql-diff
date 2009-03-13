package ru.yandex.mysqlDiff.util

trait StringImplicits {
    implicit def stringExtras(string: String) = new StringExtras(string)
}

object StringImplicits extends StringImplicits

class StringExtras(string: String) {
    def % (args: Any*) =
        String.format(string, args.toArray.asInstanceOf[Array[Object]])
}

object StringTests extends org.specs.Specification {
    import StringImplicits._
    
    "%" in {
        "distance between %s and %s is %d km" % ("Moscow", "Kiev", 757) must_== "distance between Moscow and Kiev is 757 km"
    }
}

// vim: set ts=4 sw=4 et:
