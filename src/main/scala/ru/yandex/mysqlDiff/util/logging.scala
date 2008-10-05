package ru.yandex.mysqlDiff.util

import org.slf4j._

trait Logging {
    protected def loggerName = this.getClass.getName.replaceFirst("\\$.*", "")
    val logger = LoggerFactory.getLogger(loggerName)
}

// vim: set ts=4 sw=4 et:
