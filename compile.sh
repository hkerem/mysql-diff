#!/bin/zsh -e

mkdir -p target/classes

scalac -d target/classes src/main/scala/**/*.scala

# vim: set ts=4 sw=4 et:
