#!/usr/bin/env bash

# get where this script is
SCRIPT_PATH="$(realpath $(dirname ${BASH_SOURCE[0]}))"

set -e

if [[ "$#" -lt 3 ]]; then
    echo "Syntax: $0 download_dir build_dir install_dir"
    exit 1
fi

# dependency download path
DOWNLOAD="$(realpath $1)"
mkdir -p "${DOWNLOAD}"

BUILD="$(realpath $2)"
mkdir -p "${BUILD}"

# dependency install path
INSTALL="$(realpath $3)"
mkdir -p "${INSTALL}"

export SCRIPT_PATH
export DOWNLOAD
export BUILD
export INSTALL

echo "Installing C-Thread-Pool"
. ${SCRIPT_PATH}/thpool.sh

echo "Installing SQLite3"
. ${SCRIPT_PATH}/sqlite3.sh

echo "Installing SQLITE3 PCRE"
. ${SCRIPT_PATH}/sqlite3_pcre.sh

echo "Installing GoogleTest"
. ${SCRIPT_PATH}/googletest.sh

echo "Installing Paramiko"
. ${SCRIPT_PATH}/paramiko.sh
