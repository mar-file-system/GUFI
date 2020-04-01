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

# output directories
SRCDIR="prefix"
TRACE="trace"
BADTRACE="badtrace"

# trace delimiter
DELIM="|"
BADDELIM="?"

function cleanup {
    rm -rf "${SRCDIR}" "${TRACE}" "${TRACE}".* "${BADTRACE}"
}

trap cleanup EXIT

cleanup

export LC_ALL=C

OUTPUT="verifytrace.out"

function replace() {
    echo "$@" | sed "s/${GUFI_DIR2TRACE//\//\\/}/gufi_dir2trace/g; s/${GUFI_TRACE2INDEX//\//\\/}/gufi_trace2index/g; s/${TRACE//\//\\/}\\///g; s/${SRCDIR//\//\\/}\\///g"
}

(
replace "# generate the tree"
replace "$ generatetree ${SRCDIR}"
${ROOT}/test/regression/generatetree "${SRCDIR}" 2> /dev/null
echo

replace "# generate the trace"
replace "$ ${GUFI_DIR2TRACE} -d \"${DELIM}\" -n 2 \"${SRCDIR}\" \"${TRACE}\""
${GUFI_DIR2TRACE} -d "${DELIM}" -n 2 "${SRCDIR}" "${TRACE}" 2> /dev/null
cat ${TRACE}.* > "${TRACE}"
echo

# replace the numeric fields in the trace with 0s
sed -i 's/[[:digit:]]\+/0/g' "${TRACE}"

# replace some rows with fixed data
sed -i "s/^${DELIM}d${DELIM}.*\$/${DELIM}d${DELIM}1${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}0${DELIM}/g" "${TRACE}"
sed -i "s/^leaf_directory${DELIM}d${DELIM}.*\$/leaf_directory${DELIM}d${DELIM}4${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}1${DELIM}/g" "${TRACE}"
sed -i "s/^directory${DELIM}d${DELIM}.*\$/directory${DELIM}d${DELIM}2${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}1${DELIM}/g" "${TRACE}"
sed -i "s/^directory\\/subdirectory${DELIM}d${DELIM}.*\$/directory\\/subdirectory${DELIM}d${DELIM}3${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}0${DELIM}${DELIM}${DELIM}2${DELIM}/g" "${TRACE}"

replace "# valid trace"
replace "$ verifytrace \"${DELIM}\" ${TRACE}"
replace $(${ROOT}/contrib/verifytrace "${DELIM}" "${TRACE}")
echo

replace "# wrong delimiter used"
replace "$ verifytrace \"${BADDELIM}\" ${TRACE}"
output=$(${ROOT}/contrib/verifytrace "${BADDELIM}" "${TRACE}" 2>&1)
replace "${output}"
echo

# get a file line, without the name
file_line=$(grep "${DELIM}f${DELIM}" "${TRACE}" | sort | tail -n 1)
without_name=$(echo "${file_line}" | cut -f2- -d "${DELIM}")

replace "# trace starts with a file"
echo "file${DELIM}${without_name}" > "${BADTRACE}"
replace "$ verifytrace \"${DELIM}\" ${BADTRACE}"
output=$(${ROOT}/contrib/verifytrace "${DELIM}" "${BADTRACE}" 2>&1)
replace "${output}"
echo

replace "# too few columns"
echo "${without_name}" > "${BADTRACE}"
replace "$ verifytrace \"${DELIM}\" ${BADTRACE}"
output=$(${ROOT}/contrib/verifytrace "${DELIM}" "${BADTRACE}" 2>&1)
replace "${output}"
echo

replace "# directory followed by file not in directory"
dir_line=$(grep "${DELIM}d${DELIM}" "${TRACE}" | sort | tail -n 1)
dir_name=$(echo "${dir_line}" | awk -F "${DELIM}" '{print $1}')
bad_name=$(echo "${dir_name}/non-existant_dir/file${DELIM}${without_name}")
(echo "${dir_line}"; echo "${bad_name}") > "${BADTRACE}"
output=$(${ROOT}/contrib/verifytrace "${DELIM}" "${BADTRACE}" 2>&1)
replace "${output}"
echo

replace "# bad directory pinode"
head -n 1 "${TRACE}" | awk -F "${DELIM}" '{$23="bad_pinode"; print}' OFS="${DELIM}" > "${BADTRACE}"
output=$(${ROOT}/contrib/verifytrace "${DELIM}" "${BADTRACE}" 2>&1)
replace "${output}"
echo

replace "# bad child pinode"
head -n 1 "${TRACE}" > "${BADTRACE}"
echo "${file_line}"| awk -F "${DELIM}" '{$23="1234"; print}' OFS="${DELIM}" >> "${BADTRACE}"
output=$(${ROOT}/contrib/verifytrace "${DELIM}" "${BADTRACE}" 2>&1)
replace "${output}"
echo

) | tee "${OUTPUT}"

diff ${ROOT}/test/regression/verifytrace.expected "${OUTPUT}"
rm "${OUTPUT}"
