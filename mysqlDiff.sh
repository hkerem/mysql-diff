#!/bin/bash
java -classpath ./target/dist/mysqlDiff-0.0.jar:./lib/scala/scala-library-2.7.0.jar:./lib/mysql/mysql-connector-java-5.1.6-bin.jar ru.yandex.mysqlDiff.Diff $1 $2

