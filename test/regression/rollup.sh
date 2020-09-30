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

GUFI_DIR2INDEX="${ROOT}/src/gufi_dir2index"
ROLLUP="${ROOT}/src/rollup"
GUFI_QUERY="${ROOT}/src/gufi_query"
GUFI_FIND="${ROOT}/test/regression/gufi_find.py"
GUFI_LS="${ROOT}/test/regression/gufi_ls.py"
GUFI_STATS="${ROOT}/test/regression/gufi_stats.py"

TMP="tmp"
SRCDIR="prefix"
INDEXROOT="${SRCDIR}.gufi"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"

OUTPUT="rollup.out"

function cleanup() {
    rm -rf "${TMP}" "${SRCDIR}" "${INDEXROOT}"
}

trap cleanup EXIT

cleanup

ID1="1001"
if id -un 1001 > /dev/null 2>&1;
then
    ID1="$(id -un 1001)"
fi

ID2="1002"
if id -un 1002 > /dev/null 2>&1;
then
    ID2="$(id -un 1002)"
fi

ID3="1003"
if id -un 1003 > /dev/null 2>&1;
then
    ID3="$(id -un 1003)"
fi

function replace() {
    echo "$@" | sed "s/[[:space:]]*$//g; s/${GUFI_QUERY//\//\\/}/gufi_query/g; s/${GUFI_FIND//\//\\/}/gufi_find/g; s/${GUFI_LS//\//\\/}/gufi_ls/g; s/${GUFI_STATS//\//\\/}/gufi_stats/g; s/${INDEXROOT//\//\\/}\\//prefix.gufi\\//g; s/\\/${SRCDIR//\//\\/}/./g; s/${ID1}/1001/g; s/${ID2}/1002/g; s/${ID3}/1003/g"
}

function run() {
    replace "$ $@"
    replace "$($@)"
    echo
}

(
# 17 dirs: ${TMP} + [o+rx, ugo, ug, u] + (3 subdirs * [o+rx, ugo, ug, u])
#     1 not rolled up: ${TMP}
#     16 rolled up: [o+rx, ugo, ug, u] + (3 subdirs * [o+rx, ugo, ug, u])
#
# 24 files: (6 files * [o+rx, ugo, ug, u])
mkdir ${TMP}                    # 0
mkdir -m 005 ${TMP}/o+rx        #  1
mkdir -m 707 ${TMP}/o+rx/dir1   #   1
touch ${TMP}/o+rx/dir1/file1
mkdir -m 075 ${TMP}/o+rx/dir2   #   1
touch ${TMP}/o+rx/dir2/file1
touch ${TMP}/o+rx/dir2/file2
mkdir -m 007 ${TMP}/o+rx/dir3   #   1
touch ${TMP}/o+rx/dir3/file1
touch ${TMP}/o+rx/dir3/file2
touch ${TMP}/o+rx/dir3/file3
mkdir -m 776 ${TMP}/ugo         #  4
mkdir -m 576 ${TMP}/ugo/dir1    #   1
touch ${TMP}/ugo/dir1/file1
mkdir -m 756 ${TMP}/ugo/dir2    #   1
touch ${TMP}/ugo/dir2/file1
touch ${TMP}/ugo/dir2/file2
mkdir -m 774 ${TMP}/ugo/dir3    #   1
touch ${TMP}/ugo/dir3/file1
touch ${TMP}/ugo/dir3/file2
touch ${TMP}/ugo/dir3/file3
mkdir -m 770 ${TMP}/ug          #  2
mkdir -m 750 ${TMP}/ug/dir1     #   1
touch ${TMP}/ug/dir1/file1
mkdir -m 773 ${TMP}/ug/dir2     #   1
touch ${TMP}/ug/dir2/file1
touch ${TMP}/ug/dir2/file2
mkdir -m 770 ${TMP}/ug/dir3     #   1
touch ${TMP}/ug/dir3/file1
touch ${TMP}/ug/dir3/file2
touch ${TMP}/ug/dir3/file3
mkdir -m 700 ${TMP}/u           #  3
mkdir -m 770 ${TMP}/u/dir1      #   1
touch ${TMP}/u/dir1/file1
mkdir -m 703 ${TMP}/u/dir2      #   1
touch ${TMP}/u/dir2/file1
touch ${TMP}/u/dir2/file2
mkdir -m 700 ${TMP}/u/dir3      #   1
touch ${TMP}/u/dir3/file1
touch ${TMP}/u/dir3/file2
touch ${TMP}/u/dir3/file3

# copy ${TMP} 3 times to different users
# 4 not rolled up: ${SRCDIR} + (3 * ${TMP})
# 48 rolled up: (3 * 16 dirs in ${TMP}
# 72 files: 3 * 24 files in ${TMP}
mkdir -m 777 ${SRCDIR}
cp -R ${TMP} ${SRCDIR}/1001
chown -R 1001:1001 ${SRCDIR}/1001
cp -R ${TMP} ${SRCDIR}/1002
chown -R 1002:1002 ${SRCDIR}/1002
cp -R ${TMP} ${SRCDIR}/1003
chown -R 1003:1003 ${SRCDIR}/1003
rm -r ${TMP}

${GUFI_DIR2INDEX} ${SRCDIR} ${INDEXROOT}
${ROLLUP} ${INDEXROOT} 2>&1 | head -n -1

# get results from querying
replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT path(name) from summary\" \"${INDEXROOT}\" | wc -l"
${GUFI_QUERY} -d " " -S "SELECT path(name) from summary" "${INDEXROOT}" | wc -l
echo

replace "$ ${GUFI_QUERY} -d \" \" -E \"SELECT path(summary.name) || '/' || pentries.name from summary, pentries WHERE summary.inode == pentries.pinode\" \"${INDEXROOT}\" | wc -l"
${GUFI_QUERY} -d " " -E "SELECT path(summary.name) || '/' || pentries.name from summary, pentries WHERE summary.inode == pentries.pinode" "${INDEXROOT}" | wc -l
echo

replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT path(name) from summary\" -E \"SELECT path(summary.name) || '/' || pentries.name from summary, pentries WHERE summary.inode == pentries.pinode\" \"${INDEXROOT}\" | wc -l"
${GUFI_QUERY} -d " " -S "SELECT path(name) from summary" -E "SELECT path(summary.name) || '/' || pentries.name from summary, pentries WHERE summary.inode == pentries.pinode" "${INDEXROOT}" | wc -l
echo

replace "$ ${GUFI_FIND} -type d | wc -l"
${GUFI_FIND} -type d | wc -l
echo

replace "$ ${GUFI_FIND} -type f | wc -l"
${GUFI_FIND} -type f | wc -l
echo

replace "$ ${GUFI_FIND} | wc -l"
${GUFI_FIND} | wc -l
echo

# gufi_ls
run "${GUFI_LS}"
run "${GUFI_LS} 1001"
echo "# nothing (for now) because 1001/o+rx was rolled up, and level() could not get to 1"
run "${GUFI_LS} 1001/o+rx"
run "${GUFI_LS} 1001/o+rx/dir1"
run "${GUFI_LS} 1001/o+rx/dir2"
run "${GUFI_LS} 1001/o+rx/dir3"
run "${GUFI_LS} 1002"
run "${GUFI_LS} 1002/ugo/dir1"
run "${GUFI_LS} 1002/ugo/dir2"
run "${GUFI_LS} 1002/ugo/dir3"
run "${GUFI_LS} 1003"
run "${GUFI_LS} 1003/ug/dir1"
run "${GUFI_LS} 1003/ug/dir2"
run "${GUFI_LS} 1003/ug/dir3"

echo "# 1 less because gufi_ls does not list the input dir"
replace  $ "${GUFI_LS} -R | wc -l"
${GUFI_LS} -R | wc -l
echo

# gufi_stats
run "${GUFI_STATS}    depth"
run "${GUFI_STATS} -r depth"

run "${GUFI_STATS}    filecount"
run "${GUFI_STATS} -r filecount"

run "${GUFI_STATS}    total-filecount"
run "${GUFI_STATS} -c total-filecount"

run "${GUFI_STATS}    dircount"
run "${GUFI_STATS} -r dircount"

run "${GUFI_STATS}    total-dircount"
run "${GUFI_STATS} -c total-dircount"
) 2>&1 | tee "${OUTPUT}"

diff ${ROOT}/test/regression/rollup.expected "${OUTPUT}"
rm "${OUTPUT}"
