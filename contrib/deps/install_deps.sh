#!/usr/bin/env bash

# get where this script is
SCRIPT_PATH="$(realpath $(dirname ${BASH_SOURCE[0]}))"

set -e

CXX="false"
PARAMIKO="false"

# https://stackoverflow.com/a/14203146
# Bruno Bronosky
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --cxx)
    CXX="true"
    shift # past argument
    ;;
    --paramiko)
    PARAMIKO="true"
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ "$#" -lt 3 ]]; then
    echo "Syntax: $0 download_dir build_dir install_dir"
    exit 1
fi

# dependency download path
DOWNLOAD_DIR="$(realpath $1)"
mkdir -p "${DOWNLOAD_DIR}"

BUILD_DIR="$(realpath $2)"
mkdir -p "${BUILD_DIR}"

# dependency install path
INSTALL_DIR="$(realpath $3)"
mkdir -p "${INSTALL_DIR}"

export SCRIPT_PATH
export DOWNLOAD_DIR
export BUILD_DIR
export INSTALL_DIR

echo "Installing C-Thread-Pool"
. ${SCRIPT_PATH}/thpool.sh

echo "Installing SQLite3"
. ${SCRIPT_PATH}/sqlite3.sh

echo "Installing SQLITE3 PCRE"
. ${SCRIPT_PATH}/sqlite3_pcre.sh

if [[ "${CXX}" == "true" ]]; then
    echo "Installing GoogleTest"
    . ${SCRIPT_PATH}/googletest.sh
fi

if [[ "${PARAMIKO}" == "true" ]]; then
    echo "Installing Paramiko"
    . ${SCRIPT_PATH}/paramiko.sh
fi
