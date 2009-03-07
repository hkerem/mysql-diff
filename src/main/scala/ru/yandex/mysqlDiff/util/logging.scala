package ru.yandex.mysqlDiff.util

import org.slf4j._

/** Handy slf4j wrapper */
trait Logging {
    /** Values used as logger name */
    protected def loggerName = this.getClass.getName.replaceFirst("\\$.*", "")
    val logger = LoggerFactory.getLogger(loggerName)
}

// vim: set ts=4 sw=4 et:
