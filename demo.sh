#!/bin/sh -e
#
# Demo script
#

# Edit this variables; user must have full permissions in order to drop and create database
host=localhost
user=video
password=video
db1=mysql_diff_demo_1
db2=mysql_diff_demo_2
url1="jdbc:mysql://$host:3306/$db1?user=$user&password=$password"
url2="jdbc:mysql://$host:3306/$db2?user=$user&password=$password"

ec() {
    echo "# $@" >&2
    "$@"
}

evale() {
    echo "# $@" >&2
    eval "$@"
}

mysqlp() {
    mysql --user=$user --password=$password --host=$host "$@"
}

ec mysqlp -e "DROP DATABASE IF EXISTS $db1"
ec mysqlp -e "DROP DATABASE IF EXISTS $db2"
ec mysqlp -e "CREATE DATABASE $db1"
ec mysqlp -e "CREATE DATABASE $db2"

evale "mysqlp $db1 < src/test/sql/simple_from.sql"
evale "mysqlp $db2 < src/test/sql/simple_to.sql"

ec ./mysql-diff.sh "$url1" "$url2" > demo-diff.sql
cat demo-diff.sql | egrep -v -- '^--' # drop debugging comments. XXX: drop them from program

# Apply diff to the database
evale "mysqlp $db1 < demo-diff.sql"

# vim: set ts=4 sw=4 et:
