#!/bin/zsh -e

mkdir -p target/classes

/opt/lang/java/JDK/scala-2.7.0-final/bin/scalac -deprecation -d target/classes src/main/scala/**/*.scala

# vim: set ts=4 sw=4 et:
