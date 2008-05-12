#!/bin/sh -e

# Developer script to run scala with specified classpath

scala -classpath 'target/classes:lib/*' "$@"

# vim: set ts=4 sw=4 et:
