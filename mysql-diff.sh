#!/bin/bash
java -classpath ./target/dist/mysqlDiff-0.0.jar:./target/dist/scala-library.jar:./target/dist/mysql-connector-java-5.1.6-bin.jar ru.yandex.mysqlDiff.Diff "$@"

