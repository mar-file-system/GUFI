#/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

# get osxfuse from homebrew/cask
brew tap homebrew/cask
brew cask install osxfuse

# use the compiler installed by brew
export PATH="$(brew --prefix $FORMULAE)/bin:$PATH"

# use the sqlite3 installed by brew
export PKG_CONFIG_PATH="$(brew --prefix sqlite3)/lib/pkgconfig:$PKG_CONFIG_PATH"

# build and test GUFI
${SCRIPT_PATH}/build_and_test.sh
