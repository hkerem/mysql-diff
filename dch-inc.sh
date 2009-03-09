#!/bin/sh -e
#
# Increment version in debian/changelog
# Useful only inside Yandex
#

set -e

err_exit() {
    echo "$@" >&2
    exit 1
}

ec() {
    echo "$@" >&2
    "$@"
}

version=$(./version.sh)

export DEBEMAIL="buildfarm@yandex-team.ru"
export DEBFULLNAME="buildfarm"

ec dch -v "$version" -b "incver to build debian package" < /dev/null
ec sed -e '1 { s,[a-z]\+;,unstable;, }' -i debian/changelog
echo "##teamcity[buildNumber '$version']"

# vim: set ts=4 sw=4 et:
