#!/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

if [[ "$#" -lt 1 ]]; then
    echo "Syntax: $0 file"
    exit 1
fi

FILE="$1"

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Builder"
git config --global user.email "travis@travis-ci.org"

# update the origin to include the personal access token
git remote rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git

# build the tarball
mkdir build
cd build
make gary

# move to the target branch
git checkout "${TRAVIS_BRANCH}"

# move the tarball into the target branch
mv "${FILE}" ..

cd ..

# Add the tarball and commit
git add "${FILE}"
git commit --all --message "Travis CI Tarball Upload $(date)" --message "${TRAVIS_COMMIT}" --message "[ci skip]"

# Upload the tarball
git push origin "${TRAVIS_BRANCH}"

# email everyone about the update
${SCRIPT_PATH}/email.sh "gufi-lanl@lanl.gov"
