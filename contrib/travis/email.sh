#!/usr/bin/env bash

set -e

if [[ "$#" -lt 1 ]]; then
    echo "Syntax: $0 [email ...]"
    exit 1
fi

EMAILS="$@"

DATE="$(date)"
SUBJECT="GUFI Daily Build Tarball Upload at ${DATE}"
COMMIT_MESSAGE=$(echo -e "${TRAVIS_COMMIT_MESSAGE}" | sed 's/^/    /')
mail -s "${SUBJECT}" ${EMAILS} <<EOF
This tarball was built using '${TRAVIS_BRANCH}' branch at

${COMMIT_MESSAGE}

The tarball can be downloaded at

    https://github.com/mar-file-system/GUFI/blob/master/gufi.tar.gz

EOF
