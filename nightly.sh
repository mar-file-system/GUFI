#!/usr/bin/env bash

# https://gist.github.com/willprice/e07efd73fb7f13f917ea

# Create a Travis CI git user
git config --global user.name  "Travis CI Nightly Build"
git config --global user.email "travis@travis-ci.org"

git add "${NIGHTLY}"
git commit --message "Travis CI Nightly Build $(date)" --message "[ci skip]"

# Upload the nightly build
git remove rm origin
git remote add origin https://${GH_TOKEN}@github.com/mar-file-system/GUFI.git
git push --set-upstream origin master
