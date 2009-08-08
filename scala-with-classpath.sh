#!/bin/sh -e

# Developer script to run scala with specified classpath

base() {
    JAVA_OPTS=-Xss10m java -classpath `dirname $0`/target/classes:`dirname $0`/lib/dep/'*' "$@"
}

if [ "$#" = 0 ]; then
    base scala.tools.nsc.MainGenericRunner -nocompdaemon
else
    base "$@"
fi

# vim: set ts=4 sw=4 et:
