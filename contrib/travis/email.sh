#!/usr/bin/env bash

set -e

if [[ "$#" -lt 2 ]]; then
    echo "Syntax: $0 timestamp [email ...]"
    exit 1
fi

DATE="$1"
shift
EMAILS="$@"

SUBJECT="GUFI Tarball Upload ${DATE}"
COMMIT_MESSAGE=$(echo -e "${TRAVIS_COMMIT_MESSAGE}" | sed 's/^/    /')
mail -s "${SUBJECT}" ${EMAILS} <<EOF
The tarball was built using '${TRAVIS_BRANCH}' branch at

${COMMIT_MESSAGE}

The tarball can be downloaded at

    https://github.com/mar-file-system/GUFI/raw/tarball/gufi.tar.gz

EOF
