#!/bin/zsh -e

mkdir -p target/classes

if [ "$SCALA_HOME" = "" ]
then
    echo 'Set SCALA_HOME environment variable to your scala jdk path'
    exit 1
fi


#-Xprint:refchecks 
$SCALA_HOME/bin/scalac -deprecation   -Xprint:refchecks  -classpath lib/**/*.jar -d target/classes src/main/scala/**/*.scala 

#ant -Dscala.nome=$SCALA_HOME

# vim: set ts=4 sw=4 et:
