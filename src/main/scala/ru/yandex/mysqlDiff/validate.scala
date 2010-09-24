package ru.yandex.mysqlDiff

abstract class Assume {
    def fail(message: => String = "condition failed"): Nothing
    
    def isTrue(cond: Boolean, message: => String = "condition failed") =
        if (!cond)
            fail(message)
    
    def notNull(o: AnyRef, message: => String = "must not be null") =
        isTrue(o != null, message)
}

object Validate extends Assume {
    override def fail(message: => String) = throw new IllegalArgumentException(message)
}

object Check extends Assume {
    override def fail(message: => String) = throw new IllegalStateException(message)
}

// vim: set ts=4 sw=4 et:
