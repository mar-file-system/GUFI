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
GUFI_DIR2TRACE="${ROOT}/src/gufi_dir2trace"
GUFI_TRACE2INDEX="${ROOT}/src/gufi_trace2index"
GUFI_QUERY="${ROOT}/src/gufi_query"

SRCDIR="prefix"
INDEXROOT="${SRCDIR}.gufi"
TRACE="${SRCDIR}.trace"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"

OUTPUT="xattrs.out"

function cleanup() {
    rm -rf "${TMP}" "${SRCDIR}" "${INDEXROOT}" "${TRACE}.0"
}

trap cleanup EXIT

cleanup

function replace() {
    echo "$@" | sed "s/[[:space:]]*$//g; s/${GUFI_DIR2INDEX//\//\\/}/gufi_dir2index/g; s/${GUFI_DIR2TRACE//\//\\/}/gufi_dir2trace/g; s/${GUFI_TRACE2INDEX//\//\\/}/gufi_trace2index/g; s/${GUFI_QUERY//\//\\/}/gufi_query/g; s/${INDEXROOT//\//\\/}\\//prefix.gufi\\//g; s/\\/${SRCDIR//\//\\/}/./g;"
}

function create() {
    # run in subshell
    (
        name="$1"
        perms="$2"
        mkdir -m "${perms}" "${name}"
        setfattr -n "user.${name}" -v "${perms}" "${name}"
        cd "${name}"

        # good
        touch ugo
        setfattr -n "user.ugo" -v "666" ugo
        chmod 666 ugo

        touch ug
        setfattr -n "user.ug"  -v "660" ug
        chmod 660 ug

        touch u
        setfattr -n "user.u"   -v "600" u
        chmod 600 u

        # bad
        touch o
        setfattr -n "user.o"   -v "006" o
        chmod 006 o

        touch go
        setfattr -n "user.go"  -v "066" go
        chmod 066 go
    )
}

(
    mkdir -p -m 555 "${SRCDIR}"
    cd "${SRCDIR}"

    # Rule 1: File is 0+R (doesn’t matter what the parent dir perms or ownership is)
    # create o+r 005

    # Rule 2: File is UG+R doesn’t matter on other, with file and parent same usr and grp and parent has only UG+R with no other read
    create ug+r 550

    # Rule 3: File is U+R doesn’t matter on grp and other, with file and parent same usr and parent dir has only U+R, no grp and other read
    create u+r 500

    # Rule 4: Directory has write for every read: drw*rw_*rw* or drw*rw*___ or drw*______ - if you can write the dir you can chmod the files to see the xattrs
    create ugo+rw 777
    create ug+rw  770
    create u+rw   700
)

(
replace "$ ${GUFI_DIR2INDEX} -x \"${SRCDIR}\" \"${INDEXROOT}\""
${GUFI_DIR2INDEX} -x "${SRCDIR}" "${INDEXROOT}"
echo

replace "$ ${GUFI_QUERY} -d \" \" -M -S \"SELECT path(''), summary.type, xattrs.name, xattrs.value FROM summary LEFT JOIN xattrs ON summary.inode == xattrs.inode\" -E \"SELECT path('') || '/' || pentries.name, pentries.type, xattrs.name, xattrs.value FROM pentries LEFT JOIN xattrs ON pentries.inode == xattrs.inode\" \"${INDEXROOT}\" | sort"
${GUFI_QUERY} -d " " -M -S "SELECT path(''), summary.type, xattrs.name, xattrs.value FROM summary LEFT JOIN xattrs ON summary.inode == xattrs.inode" -E "SELECT path('') || '/' || pentries.name, pentries.type, xattrs.name, xattrs.value FROM pentries LEFT JOIN xattrs ON pentries.inode == xattrs.inode" "${INDEXROOT}" | sort
echo

rm -r "${INDEXROOT}"

replace "$ ${GUFI_DIR2TRACE} -x \"${SRCDIR}\" \"${TRACE}\""
${GUFI_DIR2TRACE} -x "${SRCDIR}" "${TRACE}"
echo

replace "$ ${GUFI_TRACE2INDEX} \"${TRACE}\" \"${INDEXROOT}\""
${GUFI_TRACE2INDEX} "${TRACE}.0" "${INDEXROOT}" | tail -n 4 | head -n 3
echo

replace "$ ${GUFI_QUERY} -d \" \" -M -S \"SELECT path(''), summary.type, xattrs.name, xattrs.value FROM summary LEFT JOIN xattrs ON summary.inode == xattrs.inode\" -E \"SELECT path('') || '/' || pentries.name, pentries.type, xattrs.name, xattrs.value FROM pentries LEFT JOIN xattrs ON pentries.inode == xattrs.inode\" \"${INDEXROOT}\" | sort"
${GUFI_QUERY} -d " " -M -S "SELECT path(''), summary.type, xattrs.name, xattrs.value FROM summary LEFT JOIN xattrs ON summary.inode == xattrs.inode" -E "SELECT path('') || '/' || pentries.name, pentries.type, xattrs.name, xattrs.value FROM pentries LEFT JOIN xattrs ON pentries.inode == xattrs.inode" "${INDEXROOT}" | sort
) | tee "${OUTPUT}"

diff -b ${ROOT}/test/regression/xattrs.expected "${OUTPUT}"
rm "${OUTPUT}"
