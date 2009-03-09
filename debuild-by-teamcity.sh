#!/bin/sh -e
#
# Build debian package from TeamCity.
# Useful only inside Yandex.
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

export TZ=Europe/Moscow

cd $(dirname $0)
ec sh dch-inc.sh
ec sudo /usr/lib/pbuilder/pbuilder-satisfydepends
ec debuild -us
ec debrelease --force --nomail --to common

# vim: set ts=4 sw=4 et:
