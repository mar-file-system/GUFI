#!/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

# build and test GUFI
${SCRIPT_PATH}/build_and_test.sh
