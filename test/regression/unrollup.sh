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
UNROLLUP="${ROOT}/src/unrollup"
GUFI_QUERY="${ROOT}/src/gufi_query"

TMP="tmp"
SRCDIR="prefix"
INDEXROOT="${SRCDIR}.gufi"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"

OUTPUT="unrollup.out"

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
    echo "$@" | sed "s/[[:space:]]*$//g; s/${GUFI_DIR2INDEX//\//\\/}/gufi_dir2index/g; s/${ROLLUP//\//\\/}/rollup/g; s/${UNROLLUP//\//\\/}/unrollup/g; s/${GUFI_QUERY//\//\\/}/gufi_query/g; s/${INDEXROOT//\//\\/}\\//prefix.gufi\\//g; s/\\/${SRCDIR//\//\\/}/./g; s/${ID1}/1001/g; s/${ID2}/1002/g; s/${ID3}/1003/g"
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
mkdir ${SRCDIR}
cp -R ${TMP} ${SRCDIR}/1001
chown -R 1001:1001 ${SRCDIR}/1001
cp -R ${TMP} ${SRCDIR}/1002
chown -R 1002:1002 ${SRCDIR}/1002
cp -R ${TMP} ${SRCDIR}/1003
chown -R 1003:1003 ${SRCDIR}/1003
rm -r ${TMP}

# generate the index
run ${GUFI_DIR2INDEX} ${SRCDIR} ${INDEXROOT}

# roll up the index
run ${ROLLUP} ${INDEXROOT} 2>&1 | head -n -1

echo "# rolled up directories should have pentries tables"
replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT type FROM qlite_master where name == 'pentries'\""
output=$(${GUFI_QUERY} -d " " -S "SELECT (SELECT type FROM sqlite_master where name == 'pentries'), (SELECT path(name) FROM summary)" ${INDEXROOT})
replace "${output}"
echo

# unroll up the index
run ${UNROLLUP} ${INDEXROOT}

echo "# COUNT(entries), COUNT(restored PENTRIES), view, directory"
replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT (SELECT COUNT(*) FROM entries), (SELECT COUNT(*) FROM pentries), (SELECT type FROM sqlite_master where name == 'pentries'), path(name) FROM summary\" ${INDEXROOT}"
output=$(${GUFI_QUERY} -d " " -S "SELECT (SELECT COUNT(*) FROM entries), (SELECT COUNT(*) FROM pentries), (SELECT type FROM sqlite_master where name == 'pentries'), path(name) FROM summary" ${INDEXROOT})
replace "${output}"
echo
) 2>&1 | tee "${OUTPUT}"

diff ${ROOT}/test/regression/unrollup.expected "${OUTPUT}"
rm "${OUTPUT}"
