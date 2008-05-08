#!/bin/sh -e

HOME=/root

./depot-scripts/build-r.sh
./depot-scripts/upload-r.sh yandex-mysql-diff

# vim: set ts=4 sw=4 et:
