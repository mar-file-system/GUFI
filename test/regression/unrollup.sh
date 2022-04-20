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
    echo "$@" | sed "s/[[:space:]]*$//g; s/${GUFI_DIR2INDEX//\//\\/}/gufi_dir2index/g; s/${ROLLUP//\//\\/}/rollup/g; s/${UNROLLUP//\//\\/}/unrollup/g; s/${GUFI_QUERY//\//\\/}/gufi_query/g; s/${INDEXROOT//\//\\/}\\//prefix.gufi\\//g; s/\\/${SRCDIR//\//\\/}/./g;"
}

function run() {
    replace "$ $@"
    replace "$($@)"
    echo
}

(

# Level 0                 prefix/
#       Total 1 directory
#
# Level 1  007 o+rx | ugo 777   | ug 770    | 700 u
#          fail     | success   | fail      | fail
#
#       Total 4 directories; 1 will roll up
#
# Level 2
#          o+rx 007 | o+rx 007  | o+rx 007  | o+rx 007
#          ugo  777 | ugo  777  | ugo  777  | ugo  777
#          ug   770 | ug   770  | ug   770  | ug   770
#          u    700 | u    700  | u    700  | u    700
#
#       Total 4 x 4 = 16 directories
#       All directories should roll up
#
# Level 3
#       Total 4 x 4 x      3      = 48 directories
#       Total 4 x 4 x (1 + 2 + 3) = 96 files
#
# Total     = 1 + 4 + 16 + 48 = 69 directories
# Rolled up =     1 +  4 + 48 = 53 directories (52 subdirectories)
# Remaining = 1 + 3 + 12      = 16 directories
#

function setup() {
    parent="$1"
    perms="$2"
    count="$3"

    dir="${parent}/dir${count}"

    mkdir -m "${perms}" "${dir}"
    for i in $(seq "${count}")
    do
        touch "${dir}/file${i}"
    done
}

mkdir ${TMP}                    # Level 1

mkdir -m 007 ${TMP}/o+rx        # Level 2 007
setup ${TMP}/o+rx 777 1         # Level 3 777 - 1 file
setup ${TMP}/o+rx 077 2         # Level 3 077 - 2 files
setup ${TMP}/o+rx 007 3         # Level 3 007 - 3 files

mkdir -m 777 ${TMP}/ugo         # Level 2 777
setup ${TMP}/ugo 577 1          # Level 3 577 - 1 file
setup ${TMP}/ugo 757 2          # Level 3 757 - 2 files
setup ${TMP}/ugo 775 3          # Level 3 775 - 3 files

mkdir -m 770 ${TMP}/ug          # Level 2 770
setup ${TMP}/ug 777 1           # Level 3 577 - 1 file
setup ${TMP}/ug 770 2           # Level 3 757 - 2 files
setup ${TMP}/ug 750 3           # Level 3 775 - 3 files

mkdir -m 700 ${TMP}/u           # Level 2 700
setup ${TMP}/u 777 1            # Level 3 777 - 1 file
setup ${TMP}/u 700 2            # Level 3 700 - 2 files
setup ${TMP}/u 500 3            # Level 3 500 - 3 files

mkdir -m 777 ${SRCDIR}          # Level 0

cp -R ${TMP} ${SRCDIR}/o+rx     # Level 1
chmod 007 ${SRCDIR}/o+rx

cp -R ${TMP} ${SRCDIR}/ugo      # Level 1
chmod 777 ${SRCDIR}/ugo

cp -R ${TMP} ${SRCDIR}/ug       # Level 1
chmod 770 ${SRCDIR}/ug

cp -R ${TMP} ${SRCDIR}/u        # Level 1
chmod 700 ${SRCDIR}/u

rm -r ${TMP}

# generate the index
run ${GUFI_DIR2INDEX} ${SRCDIR} ${INDEXROOT}

# roll up the index
run ${ROLLUP} ${INDEXROOT}

echo "# rolled up directories should have pentries tables"
replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT (SELECT type FROM sqlite_master where name == 'pentries'), (SELECT path(name) FROM summary) AS fullpath ORDER BY fullpath ASC\" | sort -k2 -k1"
output=$(${GUFI_QUERY} -d " " -S "SELECT (SELECT type FROM sqlite_master where name == 'pentries'), (SELECT path(name) FROM summary) AS fullpath ORDER BY fullpath ASC" ${INDEXROOT} | sort -k2 -k1)
replace "${output}"
echo

# unroll up the index
run ${UNROLLUP} ${INDEXROOT}

echo "# COUNT(entries), COUNT(restored PENTRIES), view, directory"
replace "$ ${GUFI_QUERY} -d \" \" -S \"SELECT (SELECT COUNT(*) FROM entries), (SELECT COUNT(*) FROM pentries), (SELECT type FROM sqlite_master where name == 'pentries'), path(name) AS fullpath FROM summary ORDER BY fullpath ASC\" ${INDEXROOT} | sort -k4 -k3"
output=$(${GUFI_QUERY} -d " " -S "SELECT (SELECT COUNT(*) FROM entries), (SELECT COUNT(*) FROM pentries), (SELECT type FROM sqlite_master where name == 'pentries'), path(name) AS fullpath FROM summary ORDER BY fullpath ASC" ${INDEXROOT} | sort -k4 -k3)
replace "${output}"
echo
) | tee "${OUTPUT}"

diff ${ROOT}/test/regression/unrollup.expected "${OUTPUT}"
rm "${OUTPUT}"
