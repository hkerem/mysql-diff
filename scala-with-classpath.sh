#!/bin/sh -e

# Developer script to run scala with specified classpath

JAVA_OPTS=-Xss10m scala -classpath `dirname $0`/target/classes:`dirname $0`/lib/'*' "$@"

# vim: set ts=4 sw=4 et:
