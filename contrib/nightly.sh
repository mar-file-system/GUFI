#!/usr/bin/env bash

set -e

if [[ "$@" -lt 2 ]]; then
    echo "Syntax: $0 branch path [path ...]"
    exit 1
fi

BRANCH="$1"
shift
PATHS="$@"

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Nightly Build"
git config --global user.email "travis@travis-ci.org"

# update the origin to include the personal access token
git remote rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git

# Add the tarball and commit
git checkout "${BRANCH}"
git add ${PATHS}
git commit --all --message "Travis CI Nightly Build Tarball Upload $(date)" --message "[ci skip]"

# Upload the tarball
git push origin "${BRANCH}"
