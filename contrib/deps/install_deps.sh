#!/usr/bin/env bash
# This file is part of GUFI, which is part of MarFS, which is released
# under the BSD license.
#
#
# Copyright (c) 2017, Los Alamos National Security (LANS), LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#
# From Los Alamos National Security, LLC:
# LA-CC-15-039
#
# Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
# Copyright 2017. Los Alamos National Security, LLC. This software was produced
# under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
# Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
# the U.S. Department of Energy. The U.S. Government has rights to use,
# reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
# ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
# ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
# modified to produce derivative works, such modified software should be
# clearly marked, so as not to confuse it with the version available from
# LANL.
#
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE.



# get where this script is
# shellcheck disable=SC2046,SC2086
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
mkdir -p "$1"
DOWNLOAD_DIR=$(realpath "$1")

mkdir -p "$2"
BUILD_DIR=$(realpath "$2")

# dependency install path
mkdir -p "$3"
INSTALL_DIR=$(realpath "$3")

export SCRIPT_PATH
export DOWNLOAD_DIR
export BUILD_DIR
export INSTALL_DIR
export CMAKE
export THREADS

echo "Installing SQLite3"
source "${SCRIPT_PATH}/sqlite3.sh" "${PATCH_SQLITE3_OPEN}"

echo "Installing SQLite3 PCRE"
source "${SCRIPT_PATH}/sqlite3-pcre.sh"

if [[ "${JEMALLOC}" == "true" ]]; then
    echo "Installing jemalloc"
    source "${SCRIPT_PATH}/jemalloc.sh"
fi

CMAKE_VERSION=$(cmake --version | head -n 1 | awk '{ print $3 }' )
ACCEPTABLE_VERSION=3.5
HIGHEST_VERSION=$( (echo "${CMAKE_VERSION}"; echo "${ACCEPTABLE_VERSION}") | sort -rV | head -1)

if [[ "${CMAKE_VERSION}" == "${HIGHEST_VERSION}" ]]; then
    if [[ "${BUILD_CXX}" == "true" ]]; then
        echo "Installing GoogleTest"
        source "${SCRIPT_PATH}/googletest.sh"
    fi
fi
