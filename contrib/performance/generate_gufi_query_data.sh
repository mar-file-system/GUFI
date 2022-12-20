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
COMMIT_START="HEAD"
COMMIT_END="HEAD"
COMMIT_RATE=2
COMMIT_RUNS=5

# https://stackoverflow.com/a/14203146
# Bruno Bronosky
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --commit_start)
        COMMIT_START="$2"
        shift
        ;;
    --commit_end)
        COMMIT_END="$2"
        shift
        ;;
    --commit_rate)
        COMMIT_RATE="$2"
        shift
        ;;
    --commit_runs)
        COMMIT_RUNS="$2"
        shift
        ;;
    *)  # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        ;;
esac
    shift # past flag
done

set -- "${POSITIONAL[@]}" # restore positional parameters

GUFI="$1"
EXTRACTION_SCRIPT="$2"
HASHES_DB="$3"
FULL_HASH="$4"
RAW_DATA_DB="$5"

# Arguments check
if [[ "$#" -lt 5 ]]
then
    echo "Provide gufi build path, extraction script, hashdb, full hash, raw data db" 1>&2
    exit 1
fi

function read_from_db(){
    S=$(sqlite3 "${HASHES_DB}" "SELECT S FROM gufi_command;")
    E=$(sqlite3 "${HASHES_DB}" "SELECT E FROM gufi_command;")
    tree=$(sqlite3 "${HASHES_DB}" "SELECT tree FROM gufi_command;")
    echo "-S \"${S}\" -E \"${E}\" \"${tree}\""
}

function drop_caches(){
    sync
    sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
}

# Run gufi_query on provided commit and store debug values in database
function query_commit(){
    git checkout "$1"
    make > /dev/null
    for ((i=0; i<"${COMMIT_RUNS}"; i++))
    do
        drop_caches
        "${GUFI}/src/gufi_query" $(read_from_db) 2>&1 >/dev/null | "${EXTRACTION_SCRIPT}" "${HASHES_DB}" "${FULL_HASH}" "${RAW_DATA_DB}"
    done
}

# Main
cd "${GUFI}"
COMMITS=$(git rev-list "${COMMIT_START}..${COMMIT_END}")

i=0
while IFS= read -r commit
do
    if (("${i}" % "${COMMIT_RATE}" == 0));
    then
        query_commit "${commit}"
    fi
    ((i="${i}"+1))
done <<< "${COMMITS}"
