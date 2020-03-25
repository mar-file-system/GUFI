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

GUFI_QUERY="${ROOT}/src/gufi_query"

# output directories
SRCDIR="prefix"
INDEXROOT="${SRCDIR}.gufi"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"

OUTPUT="gufi_query.out"

function replace() {
    echo "$@" | sed "s/[[:space:]]*$//g; s/${GUFI_QUERY//\//\\/}/gufi_query/g; s/${INDEXROOT//\//\\/}\\///g; s/\\/${SRCDIR//\//\\/}/./g"
}

(
export LC_ALL=C

replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT name FROM summary\" ${INDEXROOT}"
${GUFI_QUERY} -d " " -S "SELECT name FROM summary" ${INDEXROOT} | sort
echo

replace "$ ${GUFI_QUERY} -d \" \" -E "SELECT name FROM entries" ${INDEXROOT}"
${GUFI_QUERY} -d " " -E "SELECT name FROM entries" ${INDEXROOT} | sort
echo

replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT name FROM summary\" -E \"SELECT name FROM entries\" ${INDEXROOT}"
${GUFI_QUERY} -d " " -S "SELECT name FROM summary" -E "SELECT name FROM entries" ${INDEXROOT} | sort
echo

replace "$ ${GUFI_QUERY} -d \" \" -e 0 -a -I \"CREATE TABLE out(name TEXT)\" -S \"INSERT INTO out SELECT path() || '/' || name FROM summary\" -E \"INSERT INTO out SELECT path() || '/' || name FROM entries\" -J \"INSERT INTO aggregate.out SELECT * FROM out\" -G \"SELECT name FROM out ORDER BY name ASC\" ${INDEXROOT}"
output=$(${GUFI_QUERY} -d " " -e 0 -a -I "CREATE TABLE out(name TEXT)" -S "INSERT INTO out SELECT path() || '/' || name FROM summary" -E "INSERT INTO out SELECT path() || '/' || name FROM entries" -J "INSERT INTO aggregate.out SELECT * FROM out" -G "SELECT name FROM out ORDER BY name ASC" ${INDEXROOT})
replace "${output}"
echo

replace "$ ${GUFI_QUERY} -d \" \" -e 0 -a -I \"CREATE TABLE out(name TEXT)\" -S \"INSERT INTO out SELECT path() || '/' || name FROM summary\" -E \"INSERT INTO out SELECT path() || '/' || name FROM entries\" -J \"INSERT INTO aggregate.out SELECT * FROM out\" -G \"SELECT name FROM out ORDER BY name DESC\" ${INDEXROOT}"
output=$(${GUFI_QUERY} -d " " -e 0 -a -I "CREATE TABLE out(name TEXT)" -S "INSERT INTO out SELECT path() || '/' || name FROM summary" -E "INSERT INTO out SELECT path() || '/' || name FROM entries" -J "INSERT INTO aggregate.out SELECT * FROM out" -G "SELECT name FROM out ORDER BY name DESC" ${INDEXROOT})
replace "${output}"
echo

replace "$ ${GUFI_QUERY} -d \" \" -e 0 -a -I \"CREATE TABLE out(name TEXT, size INT64)\" -E \"INSERT INTO out SELECT path() || '/' || name, size FROM entries\" -J \"INSERT INTO aggregate.out SELECT * FROM out\" -G \"SELECT name FROM out ORDER BY size ASC, name ASC\" ${INDEXROOT}"
output=$(${GUFI_QUERY} -d " " -e 0 -a -I "CREATE TABLE out(name TEXT, size INT64)" -E "INSERT INTO out SELECT path() || '/' || name, size FROM entries" -J "INSERT INTO aggregate.out SELECT * FROM out" -G "SELECT name FROM out ORDER BY size ASC, name ASC" ${INDEXROOT})
replace "${output}"
echo

replace "$ ${GUFI_QUERY} -d \" \" -e 0 -a -I \"CREATE TABLE out(name TEXT, size INT64)\" -E \"INSERT INTO out SELECT path() || '/' || name, size FROM entries\" -J \"INSERT INTO aggregate.out SELECT * FROM out\" -G \"SELECT name FROM out ORDER BY size DESC, name DESC\" ${INDEXROOT}"
output=$(${GUFI_QUERY} -d " " -e 0 -a -I "CREATE TABLE out(name TEXT, size INT64)" -E "INSERT INTO out SELECT path() || '/' || name, size FROM entries" -J "INSERT INTO aggregate.out SELECT * FROM out" -G "SELECT name FROM out ORDER BY size DESC, name DESC" ${INDEXROOT})
replace "${output}"
echo
) 2>&1 | tee "${OUTPUT}"

diff -b ${ROOT}/test/regression/gufi_query.expected "${OUTPUT}"
rm "${OUTPUT}"
