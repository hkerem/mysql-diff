#!/bin/sh -e

# Developer script to run scala with specified classpath

if [ `uname` = Darwin ]; then
    JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home
fi

javacmd() {
    if [ -z $JAVA_HOME ]; then
        java "$@"
    else
        "$JAVA_HOME/bin/java" "$@"
    fi
}

base() {
    JAVA_OPTS=-Xss10m javacmd -classpath `dirname $0`/target/classes:`dirname $0`/lib/dep/'*' "$@"
}

if [ "$#" = 0 ]; then
    base scala.tools.nsc.MainGenericRunner -nocompdaemon
else
    base "$@"
fi

# vim: set ts=4 sw=4 et:
