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

GUFI_DIR2TRACE="${ROOT}/src/gufi_dir2trace"
GUFI_TRACE2INDEX="${ROOT}/src/gufi_trace2index"

# output directories
SRCDIR="prefix"
TRACE="trace"
INDEXROOT="${SRCDIR}.gufi"
BADTRACE="badtrace"

# trace delimiter
DELIM="|"

function cleanup {
    rm -rf "${SRCDIR}" "${TRACE}" "${TRACE}".* "${INDEXROOT}" "${BADTRACE}"
}

trap cleanup EXIT

cleanup

OUTPUT="verifytraceintree.out"

function replace() {
    echo "$@" | sed "s/${GUFI_DIR2TRACE//\//\\/}/gufi_dir2trace/g; s/${GUFI_TRACE2INDEX//\//\\/}/gufi_trace2index/g; s/${TRACE//\//\\/}\\///g; s/${SRCDIR//\//\\/}\\///g;"
}

(
umask 002

replace "# generate the tree"
replace "$ generatetree ${SRCDIR}"
${ROOT}/test/regression/generatetree "${SRCDIR}" 2> /dev/null
echo

replace "# generate the trace"
replace "$ ${GUFI_DIR2TRACE} -d \"${DELIM}\" -n 2 -x -o \"${TRACE}\" \"${SRCDIR}\""
${GUFI_DIR2TRACE} -d "${DELIM}" -n 2 -x "${SRCDIR}" "${TRACE}" 2> /dev/null
cat ${TRACE}.* > "${TRACE}"
echo

replace "# generate the index"
replace "$ ${GUFI_TRACE2INDEX} -d \"${DELIM}\" \"${TRACE}\" \"${INDEXROOT}\""
${GUFI_TRACE2INDEX} -d "${DELIM}" "${TRACE}" "${INDEXROOT}" | tail -n 4 | head -n 3
echo

replace "# verify that all entries in the trace can be found in the GUFI tree"
replace "$ verifytraceintree ${TRACE} \"${DELIM}\" ${INDEXROOT}"
${ROOT}/contrib/verifytraceintree "${TRACE}" "${DELIM}" "${INDEXROOT}"
echo

replace "# replace a file name"
sed "s/empty_file/an_empty_file/g" "${TRACE}" > "${BADTRACE}"
replace "$ verifytraceintree ${BADTRACE} \"${DELIM}\" ${INDEXROOT}"
${ROOT}/contrib/verifytraceintree "${BADTRACE}" "${DELIM}" "${INDEXROOT}"
echo

replace "# replace a directory name"
sed "s/subdirectory${DELIM}/subdir${DELIM}/g" "${TRACE}" > "${BADTRACE}"
replace "$ verifytraceintree ${BADTRACE} \"${DELIM}\" ${INDEXROOT}"
${ROOT}/contrib/verifytraceintree "${BADTRACE}" "${DELIM}" "${INDEXROOT}"
echo

) |& sed '/qptpool.*/d' | tee "${OUTPUT}"

diff ${ROOT}/test/regression/verifytraceintree.expected "${OUTPUT}"
rm "${OUTPUT}"
