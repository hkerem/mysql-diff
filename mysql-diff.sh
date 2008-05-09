#!/bin/sh -e

scala -classpath 'target/classes:lib/*' ru.yandex.mysqlDiff.Diff "$@"

# vim: set ts=4 sw=4 et:
