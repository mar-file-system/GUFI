#!/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

# get a newer version of libsqlite3-dev than the one provided by xenial (must be at least version 3.13)
# this ppa is not allowed through addons, so it has to be added manually
sudo add-apt-repository ppa:jonathonf/backports -y
sudo apt-get update
sudo apt-get install libsqlite3-dev -y

# make the tarball
mkdir build
cd build
cmake ..
make gary
cd ..

TARBALL=gufi.tar.gz

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Builder"
git config --global user.email "travis@travis-ci.com"

# update the origin to include the personal access token
git remote rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git

# move to the tarball branch
git fetch --all --prune
git checkout tarball

# move the tarball into the target branch
mv "build/${TARBALL}" "${TARBALL}"

# Add the tarball and commit
git add "${TARBALL}"
git commit --message "Travis CI Tarball Upload (${TRAVIS_EVENT_TYPE}) $(date)" --message "${TRAVIS_BRANCH} ${TRAVIS_COMMIT}" --message "[ci skip]"

# Upload the tarball
git push origin tarball

# copy the email script from the original branch since the tarball branch does not have it
git checkout "${TRAVIS_BRANCH}" -- ${SCRIPT_PATH}/email.sh

# email everyone about the update
${SCRIPT_PATH}/email.sh "gufi-lanl@lanl.gov"
