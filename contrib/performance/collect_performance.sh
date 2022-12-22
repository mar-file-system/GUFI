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



set -e

# DEFAULTS
RUNS=30
EXTRACT="@CMAKE_CURRENT_BINARY_DIR@/extract.py"
BUILD_THREADS="" # empty

function help() {
    echo "Syntax: $0 [options] gufi_build_path hashdb full_hash raw_data_db [commit_id or commit_range@freq...]"
    echo
    echo "Options:"
    echo "    --runs COUNT             number of times to run the GUFI executable per commit"
    echo "    --extract PATH           path of extract.py"
    echo "    --build-threads COUNT    number of threads to use when building GUFI after changing commit"
    echo
}

# https://stackoverflow.com/a/14203146
# Bruno Bronosky
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case "${key}" in
    -h|--help)
        help
        exit 0
        ;;
    --runs)
        RUNS="$2"
        shift
        ;;
    --extract)
        EXTRACT="$2"
        shift
        ;;
    --build-threads)
        BUILD_THREADS="$2"
        shift
        ;;
    *)  # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        ;;
esac

shift # past flag

done

set -- "${POSITIONAL[@]}" # restore positional parameters

# Arguments check
if [[ "$#" -lt 4 ]]
then
    help 1>&2
    exit 1
fi

GUFI="$(realpath $1)"
HASHES_DB="$(realpath $2)"
FULL_HASH="$3"
RAW_DATA_DB="$(realpath $4)"

shift 4 # shift multiple here because it doesn't matter if it silently fails

# Main
cd "${GUFI}"

# all remaining arguments are treated as commit hashes
COMMITS=()
while [[ $# -gt 0 ]]
do
    ish="$1"

    if [[ "${ish}" =~ ^.*\.\..*$ ]]
    then
        range="${ish%@*}"
        freq="${ish##*@}"

        if [[ "${freq}" == "${ish}" ]]
        then
            freq=1
        fi

        mapfile -t commits < <(git rev-list --reverse "${range}")

        i=0
        for commit in "${commits[@]}"
        do
            if ! (( i % freq ))
            then
                COMMITS+=("${commit}")
            fi

            ((i = i + 1))
        done
    else
        # shellcheck disable=SC2207
        COMMITS+=($(git rev-parse "${ish}"))
    fi

    shift
done

export PATH="@DEP_INSTALL_PREFIX@/sqlite3/bin:${PATH}"
export PYTHONPATH="@CMAKE_CURRENT_BINARY_DIR@/..:${PYTHONPATH}"

# generate the original GUFI command that was run
function generate_cmd() {
    gufi_hash="$1"

    cmd=$(sqlite3 "${HASHES_DB}" "SELECT cmd FROM gufi_command WHERE hash == \"${gufi_hash}\";")
    S=$(sqlite3 "${HASHES_DB}" "SELECT S FROM gufi_command WHERE hash == \"${gufi_hash}\";")
    E=$(sqlite3 "${HASHES_DB}" "SELECT E FROM gufi_command WHERE hash == \"${gufi_hash}\";")
    tree=$(sqlite3 "${HASHES_DB}" "SELECT tree FROM gufi_command WHERE hash == \"${gufi_hash}\";")
    echo "${GUFI}/src/${cmd} -S \"${S}\" -E \"${E}\" \"${tree}\""
}

gufi_hash=$(sqlite3 "${HASHES_DB}" "SELECT gufi_hash FROM raw_data_dbs WHERE hash == \"${FULL_HASH}\";")
gufi_cmd=$(generate_cmd "${gufi_hash}")

function drop_caches() {
    sync
    sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
}

# collect performance numbers for each commit
for commit in "${COMMITS[@]}"
do
    # switch to commit and rebuild
    git -c advice.detachedHead=false checkout "${commit}"
    # shellcheck disable=SC2046
    make -j ${BUILD_THREADS} > /dev/null

    # for a single commit, run the GUFI command multiple times
    # and put the performance numbers in the raw data database
    for ((i = 0; i < RUNS; i++))
    do
        drop_caches

        # shellcheck disable=SC2046
        ${gufi_cmd} 2>&1 >/dev/null | "${EXTRACT}" "${HASHES_DB}" "${FULL_HASH}" "${RAW_DATA_DB}"
    done
done
