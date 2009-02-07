#!/bin/sh -e

BASE=0.3

TAGS=$(hg id --tags)

if [ "$TAGS" = "" -o "$TAGS" = "tip" ]; then
    echo "$BASE-$(hg id --id)"
else
    echo "$TAGS"
fi

# vim: set ts=4 sw=4 et:
