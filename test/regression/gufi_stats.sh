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

ROOT="$(realpath ${BASH_SOURCE[0]})"
ROOT="$(dirname ${ROOT})"
ROOT="$(dirname ${ROOT})"
ROOT="$(dirname ${ROOT})"

# gufi_stats wrapper
GUFI_STATS="${ROOT}/test/regression/gufi_stats.py"

# output directories
SRCDIR="prefix"
INDEXROOT="$(realpath ${SRCDIR}.gufi)"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"

OUTPUT="gufi_stats.out"

function replace() {
    echo "$@" | sed "s/${GUFI_STATS//\//\\/}/gufi_stats/g; s/$(id -un)/1001/g"
}

# function root() {
#     replace "# $@"
#     output=$(sudo -E env "PATH=$PATH" env "PYTHONPATH=$PYTHONPATH" bash -c "$@")
#     replace "${output}"
#     echo
# }

function user() {
    replace "$ $@"
    replace "$($@)"
    echo
}

(
user "${GUFI_STATS}    depth"
user "${GUFI_STATS} -r depth"

user "${GUFI_STATS}    filesize"
user "${GUFI_STATS} -r filesize"

user "${GUFI_STATS}    filecount"
user "${GUFI_STATS} -r filecount"

user "${GUFI_STATS}    linkcount"
user "${GUFI_STATS} -r linkcount"

user "${GUFI_STATS}    total-filesize"
user "${GUFI_STATS} -c total-filesize"

user "${GUFI_STATS}    total-filecount"
user "${GUFI_STATS} -c total-filecount"

user "${GUFI_STATS}    total-linkcount"
user "${GUFI_STATS} -c total-linkcount"

user "${GUFI_STATS}    dircount"
user "${GUFI_STATS} -r dircount"

user "${GUFI_STATS}    total-dircount"
user "${GUFI_STATS} -c total-dircount"

user "${GUFI_STATS}    files-per-level"
user "${GUFI_STATS} -c files-per-level"

user "${GUFI_STATS}    links-per-level"
user "${GUFI_STATS} -c links-per-level"

user "${GUFI_STATS}    dirs-per-level"
user "${GUFI_STATS} -c dirs-per-level"

if [[ "$(uname)" != "Darwin" ]]
then
    user "${GUFI_STATS}    leaf-dirs"
    user "${GUFI_STATS} -r leaf-dirs"

    user "${GUFI_STATS}    leaf-depth"
    user "${GUFI_STATS} -r leaf-depth"

    user "${GUFI_STATS}    leaf-files"
    user "${GUFI_STATS} -r leaf-files"

    user "${GUFI_STATS}    total-leaf-files"
    user "${GUFI_STATS} -c total-leaf-files"

    user "${GUFI_STATS}    median-leaf-files"
fi

) 2>&1 | tee "${OUTPUT}"

EXPECTED="${ROOT}/test/regression/gufi_stats.expected"
COPY="${EXPECTED}.copy"

if [[ "$(uname)" == "Darwin" ]]
then
    sed '/leaf-/,$d' "${EXPECTED}" > "${COPY}"
else
    cp "${EXPECTED}" "${COPY}"
fi

diff "${COPY}" "${OUTPUT}"
rm "${OUTPUT}"
