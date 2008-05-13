#!/bin/sh -e

svn rm https://svn.yandex.ru/capital/mysql-diff/tmp/yandex-mysql-diff.jar -m 'drop prev version'
svn import target/yandex-mysql-diff.jar https://svn.yandex.ru/capital/mysql-diff/tmp/yandex-mysql-diff.jar -m 'some version'


# vim: set ts=4 sw=4 et:
