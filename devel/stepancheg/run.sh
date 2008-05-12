#!/bin/sh -e

scala -classpath ../../lib/scalax-2008-05-09.jar:. Sql "$@"

# vim: set ts=4 sw=4 et:
