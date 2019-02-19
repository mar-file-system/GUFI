#!/usr/bin/env bash

set -e

if [[ "$#" -lt 3 ]]; then
    echo "Syntax: $0 branch file destination"
    exit 1
fi

BRANCH="$1"
FILE="$2"
DEST="$3"

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Nightly Build"
git config --global user.email "travis@travis-ci.org"

# update the origin to include the personal access token
git remote rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git

# Add the tarball and commit
git checkout "${BRANCH}"
mv ${FILE} ${DEST}
git commit --all --message "Travis CI Tarball Upload $(date)" --message "[ci skip]"

# Upload the tarball
git push origin "${BRANCH}"
