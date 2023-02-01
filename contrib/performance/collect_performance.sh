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
GIT="@GIT_EXECUTABLE@"
REPO="https://github.com/mar-file-system/GUFI.git"
DEP_INSTALL_PREFIX="@DEP_INSTALL_PREFIX@"
BUILD_THREADS="" # empty
SUDO=""
DROP_CACHES=1

function help() {
    echo "Syntax: $0 [options] gufi_build_path database raw_data_hash raw_data_db [identifier]... [identifier_range[%freq]]..."
    echo
    echo  "gufi_build_path should be an immediate subdirectory of a second copy of the GUFI source"
    echo
    echo "Options:"
    echo "    --runs COUNT             number of times to run the GUFI executable per commit"
    echo "    --sudo                   run the GUFI executable with sudo"
    echo "    --extract PATH           path of extract.py"
    echo "    --git PATH               path of git executable"
    echo "    --repo URI               URI of second GUFI repo to clone"
    echo "    --dep-install-prefix     directory of GUFI dependencies to link against"
    echo "    --build-threads COUNT    number of threads to use when building GUFI after changing commit"
    echo "    --dont-drop-caches       DO NOT use this flag unless you cannot drop caches"
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
        EXTRACT="$(realpath -e $2)"
        shift
        ;;
    --git)
        GIT="$(realpath -e $2)"
        shift
        ;;
    --repo)
        REPO="$2"
        shift
        ;;
    --src-dir)
        SRCDIR2="$(realpath -m $2)"
        shift
        ;;
    --dep-install-prefix)
        DEP_INSTALL_PREFIX="$(realpath -m $2)"
        shift
        ;;
    --build-threads)
        BUILD_THREADS="$2"
        shift
        ;;
    --sudo)
        SUDO="sudo"
        ;;
    --dont-drop-caches)
        DROP_CACHES=0
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

GUFI=$(realpath -m "$1")
HASHES_DB=$(realpath -e "$2")
RAW_DATA_HASH="$3"
RAW_DATA_DB=$(realpath -e "$4")

# if the user didn't provide a source directory
if [[ -z "${SRCDIR2}" ]]
then
    # try to find source directory of second GUFI repo
    if [[ -f "${GUFI}/CMakeCache.txt" ]]
    then
        # shellcheck disable=SC2016
        SRCDIR2="$(@GREP@ SOURCE_DIR ${GUFI}/CMakeCache.txt | @AWK@ -F '=' '{ printf $2 }' )"
    else
        SRCDIR2="$(dirname ${GUFI})"
    fi
fi

# not checking if the SRCDIR2 exists - will clone later
SRCDIR2="$(realpath -m ${SRCDIR2})"

if [[ "${SRCDIR2}" == "@CMAKE_SOURCE_DIR@" ]]
then
    echo "Error: Do not point --src-dir to the current repo's root" 1>&2
    exit 1
fi

shift 4

# all remaining arguments are treated as commit ids
# the commit ids are read from CMAKE_SOURCE_DIR, not GUFI
COMMITS=()
while [[ $# -gt 0 ]]
do
    ish="$1"

    if [[ "${ish}" =~ ^.*\.\..*$ ]]
    then
        range="${ish%\%*}"
        freq="${ish##*\%}"

        if [[ "${freq}" == "${ish}" ]]
        then
            freq=1
        fi

        mapfile -t commits < <("${GIT}" -C "@CMAKE_SOURCE_DIR@" rev-list "${range}")

        i=0
        for commit in "${commits[@]}"
        do
            if ! (( i % freq ))
            then
                COMMITS+=("${commit}")
            fi

            (( i = i + 1 ))
        done
    else
        mapfile -t commits < <("${GIT}" -C "@CMAKE_SOURCE_DIR@" rev-parse "${ish}")
        COMMITS+=("${commits[@]}")
    fi

    shift
done

export PATH="@DEP_INSTALL_PREFIX@/sqlite3/bin:${PATH}"
export LD_LIBRARY_PATH="@DEP_INSTALL_PREFIX@/sqlite3/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="@CMAKE_CURRENT_BINARY_DIR@/..:${PYTHONPATH}"

# reconstruct the original command
# shellcheck disable=SC2034,SC2046,SC2154
function generate_cmd() {
    gufi_hash="$1"

    # extract command components one at a time just in case a value has spaces in it when it shouldn't
    for col in cmd x a n d outfile outdb I T S E F y z J K G m B w terse tree
    do
        # shellcheck disable=SC2229
        read -r "${col}" <<<$(sqlite3 "${HASHES_DB}" "SELECT ${col} FROM gufi_command WHERE hash == \"${gufi_hash}\";")
    done

    # booleans
    flags=""
    for flag in x a m w
    do
        if (( "${!flag}" ))
        then
            flags="${flags} -${flag}"
        fi
    done

    if (( "${terse}" ))
    then
        flags="${flags} -j"
    fi

    # flags with non-empty string representations
    for flag in n d I T S E F y z J K G B
    do
        if [[ "${!flag}" ]]
        then
            flags="${flags} -${flag} \"${!flag}\""
        fi
    done

    # command will complain if both -o and -O are set
    if [[ "${outfile}" ]]
    then
        flags="${flags} -o \"${outfile}\""
    fi
    if [[ "${outdb}" ]]
    then
        flags="${flags} -O \"${outdb}\""
    fi

    echo "${GUFI}/src/${cmd} ${flags} \"${tree}\""
}

gufi_hash=$(sqlite3 "${HASHES_DB}" "SELECT gufi_hash FROM raw_data WHERE hash == \"${RAW_DATA_HASH}\";")
gufi_cmd=$(generate_cmd "${gufi_hash}")

echo -e "Collecting numbers for\n${gufi_cmd}"

function drop_caches() {
    sync
    sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
}

if [[ -d "${SRCDIR2}" ]]
then
    # make sure existing SRCDIR2 is at least tracked by git
    "${GIT}" -C "${SRCDIR2}" rev-parse --is-inside-work-tree
else
    # if the second copy's source directory doesn't
    # exist, clone a new copy of the repo
    "${GIT}" clone "${REPO}" "${SRCDIR2}"

    mkdir -p "${GUFI}"

    # cmake --build might be bugged
    # configure the second repo
    (
        cd "${GUFI}"
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DDEP_INSTALL_PREFIX="${DEP_INSTALL_PREFIX}" -DPRINT_CUMULATIVE_TIMES=On
    )
fi

# update second repo's data
"${GIT}" -C "${SRCDIR2}" fetch --all

# collect performance numbers for each commit
for commit in "${COMMITS[@]}"
do
    # switch to commit and rebuild
    "${GIT}" -C "${SRCDIR2}" -c advice.detachedHead=false checkout "${commit}"

    # PRINT_CUMULATIVE_TIMES changes to CUMULATIVE_TIMES for commits before commit 1a6137e "group debug macros together"
    (
        cd "${GUFI}"
        cmake -DCMAKE_BUILD_TYPE=Debug -DPRINT_CUMULATIVE_TIMES=On -DCUMULATIVE_TIMES=On "${SRCDIR2}"
    )

    # if build fails, skip current commit
    # shellcheck disable=SC2046
    make -C "${GUFI}" -j ${BUILD_THREADS} > /dev/null || continue

    # for a single commit, run the command multiple times and
    # put the performance numbers in the raw data database
    for ((i = 0; i < RUNS; i++))
    do
        if [[ "${DROP_CACHES}" -eq "1" ]]
        then
            drop_caches
        fi

        # run gufi_cmd through bash to remove single quotes
        # if invalid data from commit break loop
        # shellcheck disable=SC2069
        bash -c "${SUDO} ${gufi_cmd}" 2>&1 >/dev/null | "${EXTRACT}" "${HASHES_DB}" "${RAW_DATA_HASH}" "${RAW_DATA_DB}" --commit "${commit}" || break
    done
done
