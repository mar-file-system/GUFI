#!/usr/bin/env bash

set -e

# get a newer version of libsqlite3-dev than the one provided by xenial (must be at least version 3.13)
# this ppa is not allowed through addons, so it has to be added manually
sudo add-apt-repository ppa:jonathonf/backports -y
sudo apt-get update
sudo apt-get install libsqlite3-dev -y

# build and test GUFI
contrib/build_and_test.sh
