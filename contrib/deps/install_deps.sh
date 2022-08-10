#!/usr/bin/env bash

# get where this script is
SCRIPT_PATH="$(realpath $(dirname ${BASH_SOURCE[0]}))"

set -e

#Call the right cmake binary
if [ -x "$(command -v cmake)" ]
 then
  CMAKE=$(command -v cmake)
elif [ -x "$(command -v cmake3)" ]
 then
  CMAKE=$(command -v cmake3)
else
  echo "No cmake available!"
  exit 1
fi

THREADS="1"
BUILD_CXX="false"
PARAMIKO="false"
PATCH_SQLITE3_OPEN="false"
JEMALLOC="false"

# https://stackoverflow.com/a/14203146
# Bruno Bronosky
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --threads)
        THREADS="$2"
        shift # past count
        ;;
    --cxx)
        BUILD_CXX="true"
        ;;
    --paramiko)
        PARAMIKO="true"
        ;;
    --patch-sqlite3-open)
        PATCH_SQLITE3_OPEN="true"
        ;;
    --jemalloc)
        JEMALLOC="true"
        ;;
    *)    # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        ;;
esac
    shift # past flag
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
export CMAKE
export THREADS

echo "Installing SQLite3"
. ${SCRIPT_PATH}/sqlite3.sh "${PATCH_SQLITE3_OPEN}"

echo "Installing SQLite3 PCRE"
. ${SCRIPT_PATH}/sqlite3_pcre.sh

if [[ "${JEMALLOC}" == "true" ]]; then
    echo "Installing jemalloc"
    . ${SCRIPT_PATH}/jemalloc.sh
fi

CMAKE_VERSION=$(cmake --version | grep -Po '(?<=version )[^;]+')
ACCEPTABLE_VERSION=3.5
HIGHEST_VERSION=$((echo ${CMAKE_VERSION}; echo "${ACCEPTABLE_VERSION}") | sort -rV | head -1)

if [[ "${CMAKE_VERSION}" == "${HIGHEST_VERSION}" ]]; then
    if [[ "${BUILD_CXX}" == "true" ]]; then
        echo "Installing GoogleTest"
        . ${SCRIPT_PATH}/googletest.sh
    fi
fi

if [[ "${PARAMIKO}" == "true" ]]; then
    echo "Installing Paramiko"
    . ${SCRIPT_PATH}/paramiko.sh
fi
