#!/usr/bin/env bash

set -e

SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"

if [[ "$#" -lt 2 ]]; then
    echo "Syntax: $0 file destination"
    exit 1
fi

FILE="$1"
DEST="$2"

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Nightly Build"
git config --global user.email "travis@travis-ci.org"

# update the origin to include the personal access token
git remote rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git

# Add the tarball and commit
git checkout "${TRAVIS_BRANCH}"
mv ${FILE} ${DEST}
git commit --all --message "Travis CI Tarball Upload $(date)" --message "[ci skip]"

# Upload the tarball
git push origin "${TRAVIS_BRANCH}"

# email everyone about the update
${SCRIPT_PATH}/email.sh "gufi-lanl@lanl.gov"
