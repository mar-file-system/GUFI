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

# output directories
SRCDIR="prefix"
INDEXROOT="${SRCDIR}.gufi"

function cleanup {
    rm -rf "${SRCDIR}" "${INDEXROOT}"
}

trap cleanup EXIT

cleanup

# generate the tree
${ROOT}/test/regression/generatetree "${SRCDIR}"

OUTPUT="gufi_dir2index.out"

# full index
(
# generate the index
${GUFI_DIR2INDEX} -x "${SRCDIR}" "${INDEXROOT}"

src_dirs=$(find "${SRCDIR}/" -type d -printf "%p\n" | sort)
index_dirs=$(find "${INDEXROOT}/" -type d -printf "%p\n" | sed "s/${INDEXROOT}/${SRCDIR}/g" | sort)
src_entries=$(find "${SRCDIR}/" -not -type d -printf "%p\n" | sort)
index_entries=$(${ROOT}/src/gufi_query -d " " -E "SELECT path() || '/' || name FROM entries" "${INDEXROOT}" | sed "s/${INDEXROOT}/${SRCDIR}/g" | sort)

src_combined=$((echo "${src_dirs}" ; echo "${src_entries}") | sort | awk '{ printf "        " $0 "\n" }')
index_combined=$(        (echo "${index_dirs}" ; echo "${index_entries}") | sort | awk '{ printf "        " $0 "\n" }')

echo "Index up to level ${level}:"
echo "    Source Directory:"
echo "${src_combined}"
echo
echo "    GUFI Index:"
echo "${index_combined}"
echo

diff -b <(echo "${src_combined}") <(echo "${index_combined}") && echo "No difference"
) |& tee "${OUTPUT}"

# index up to different levels of the tree
for level in 0 1 2
do
    (
        # remove preexisting indicies
        rm -rf "${INDEXROOT}"

        # generate the index
        ${GUFI_DIR2INDEX} -z ${level} -x "${SRCDIR}" "${INDEXROOT}"

        # get the directories
        src_dirs=$(find "${SRCDIR}/" -maxdepth ${level} -type d -printf "%p\n" | sort)
        index_dirs=$(find "${INDEXROOT}/" -maxdepth ${level} -type d -printf "%p\n" | sed "s/${INDEXROOT}/${SRCDIR}/g" | sort)
        src_entries=$(find "${SRCDIR}/" -maxdepth $((${level} + 1)) -not -type d -printf "%p\n" | sort)
        index_entries=$(${ROOT}/src/gufi_query -d " " -z ${level} -E "SELECT path() || '/' || name FROM entries" "${INDEXROOT}" | sed "s/${INDEXROOT}/${SRCDIR}/g" | sort)

        src_combined=$((echo "${src_dirs}" ; echo "${src_entries}") | sort | awk '{ printf "        " $0 "\n" }')
        index_combined=$(        (echo "${index_dirs}" ; echo "${index_entries}") | sort | awk '{ printf "        " $0 "\n" }')

        echo "Index up to level ${level}:"
        echo "    Source Directory:"
        echo "${src_combined}"
        echo
        echo "    GUFI Index:"
        echo "${index_combined}"
        echo

        diff -b <(echo "${src_combined}") <(echo "${index_combined}") && echo "No difference"

    ) |& tee -a "${OUTPUT}"
done
diff -b ${ROOT}/test/regression/gufi_dir2index.expected "${OUTPUT}"
rm "${OUTPUT}"
