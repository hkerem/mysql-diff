#!/bin/sh -e

errExit() {
    echo "$@" >&2
    exit 1
}

# ivy.cache.dir is to make debuild work
ANT_OPTS=-Xmx512m ant -Divy.cache.dir=$HOME/.ivy2/cache dist

DESTDIR="$1"

[ -n "$DESTDIR" ] || errExit "usage: $0 <destdir>"

mkdir -p $DESTDIR/usr/local

cp -a target/dist/mysql-diff $DESTDIR/usr/local/mysql-diff

# vim: set ts=4 sw=4 et:
