#!/bin/sh -e

# Developer script to run scala with specified classpath

JAVA_OPTS=-Xss10m scala -classpath 'target/classes:lib/*' "$@"

# vim: set ts=4 sw=4 et:
