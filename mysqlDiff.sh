#!/bin/bash
java -classpath ./target/dist/mysqlDiff-0.0.jar:./lib/scala/scala-library-2.7.0.jar ru.yandex.mysqlDiff.Diff $1 $2
