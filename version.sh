#!/bin/sh -e

BASE=0.3

TAGS=$(hg id --tags)

if [ "$TAGS" = "" -o "$TAGS" = "tip" ]; then
    echo "$BASE.$(hg log -r . --limit 1 --template '{date|rfc3339date}' | sed -e 's,+.*,,; s,[T:-],,g').$(hg log -r . --template '{node|short}')"
else
    echo "$TAGS"
fi

# vim: set ts=4 sw=4 et:
