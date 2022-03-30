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
TRACE="${SRCDIR}.trace"
INDEXROOT="${SRCDIR}.gufi"

# trace delimiter
DELIM="|"

function cleanup {
    rm -rf "${SRCDIR}" "${TRACE}" "${TRACE}".* "${INDEXROOT}"
}

trap cleanup EXIT

cleanup

export LC_ALL=C

OUTPUT="gufi_trace2index.out"

function replace() {
    echo "$@" | sed "s/${GUFI_DIR2TRACE//\//\\/}/gufi_dir2trace/g; s/${GUFI_TRACE2INDEX//\//\\/}/gufi_trace2index/g; s/${TRACE//\//\\/}\\///g; s/${SRCDIR//\//\\/}\\///g; s/[[:space:]]*$//g"
}

(

# generate the tree
replace "$ generatetree ${SRCDIR}"
${ROOT}/test/regression/generatetree "${SRCDIR}"
echo

# generate the trace
replace "$ ${GUFI_DIR2TRACE} -d \"${DELIM}\" -n 2 -x -o \"${TRACE}\" \"${SRCDIR}\""
${GUFI_DIR2TRACE} -d "${DELIM}" -n 2 -x "${SRCDIR}" "${TRACE}"
cat ${TRACE}.* > "${TRACE}"
echo

# generate the index
replace "$ ${GUFI_TRACE2INDEX} -d \"${DELIM}\" \"${TRACE}\" \"${INDEXROOT}\""
${GUFI_TRACE2INDEX} -d "${DELIM}" "${TRACE}" "${INDEXROOT}" | tail -n 3
echo

# compare contents
src_contents=$(find "${SRCDIR}" | sort)
index_contents=$(${ROOT}/src/gufi_query -d " " -S "SELECT path(name) FROM summary" -E "SELECT path((SELECT name FROM summary WHERE summary.inode == pentries.pinode)) || '/' || name FROM pentries" "${INDEXROOT}" | sed "s/${INDEXROOT}/${SRCDIR}/g; s/^[[:space:]]*//g; s/[[:space:]]*$//g; s/\\/\\//\\//g" | sort)

echo "Source Directory:"
echo "${src_contents}" | awk '{ printf "    " $0 "\n" }'
echo
echo "GUFI Index:"
echo "${index_contents}" | awk '{ printf "    " $0 "\n" }'
echo

) | tee "${OUTPUT}"

diff ${ROOT}/test/regression/gufi_trace2index.expected "${OUTPUT}"
rm "${OUTPUT}"
