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

# output directories
SRCDIR="prefix"
INDEXROOT="$(realpath ${SRCDIR}.gufi)"

source ${ROOT}/test/regression/setup.sh "${ROOT}" "${SRCDIR}" "${INDEXROOT}"
source ${ROOT}/scripts/completions "${CONFIG}"

function get_function_name() {
    echo "$@ completion function:"
    complete -p $@ | sed "s/.*-F \\([^ ]*\\) .*/\\1/"
    echo
}

# Adapted from the answer posted by bonsaiviking at
# https://stackoverflow.com/a/9505024
function tabtab() {
    # manually fill in COMP variables
    # COMP_LINE="__gufi $@"
    COMP_WORDS=("__gufi" "$@")
    COMP_CWORD=$((${#COMP_WORDS[@]} - 1))
    # COMP_POINT=$((${#COMP_LINE}))

    __gufi && echo "${COMPREPLY[*]}"
}

OUTPUT="completions.out"

(
get_function_name gufi_find
get_function_name gufi_ls
get_function_name gufi_stats

echo "Root entries:"
tabtab ""
echo

echo "Root entries starting with \".\":"
tabtab "."
echo

echo "Root entries starting with \"1\":"
tabtab 1
echo

echo "Root entries starting with \"e\":"
tabtab e
echo

echo "Root entries starting with \"w\":"
tabtab w
echo

echo "Root entries starting with \"dir\":"
tabtab dir
echo

echo "The \"directory\" directory:"
tabtab directory
echo

echo "The \"directory\" directory with a slash:"
tabtab directory/
echo

echo "Directories under directory whose names start with \"sub\":"
tabtab directory/sub
echo

echo "The \"directory/subdirectory\" directory:"
tabtab directory/subdirectory
echo
) | tee "${OUTPUT}"

diff ${ROOT}/test/regression/completions.expected "${OUTPUT}"
rm "${OUTPUT}"
