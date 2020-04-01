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




if [[ "$#" -lt 1 ]]
then
    echo "Syntax: $0 file"
    exit 1
fi

file="$1"
tmp="${file}.tmp"
thread_file="${file}.thread_ids"

export LC_ALL=C

if [[ ! -f "${tmp}" ]]
then
    # get subset of timestamp lines
    grep -E "^[0-9]+ \S* [0-9]+ [0-9]+$" "${file}" | awk '{ print $1 " " $2 }' | sort -u > "${tmp}"
fi

# get set of unique labels
label_file="${file}.labels"
if [[ ( ! -f "${label_file}" ) || ( "${file}" -nt "${label_file}" ) ]]
then
    awk '{ print $2 }' "${tmp}" | sort -u > "${label_file}"
fi

# get unique thread ids
if [[ ( ! -f "${thread_file}" ) || ( "${file}" -nt "${thread_file}" ) ]]
then
    awk '{ print $1 }' "${tmp}" | sort -un > "${thread_file}" &
fi

# separate timestamps by label
echo "Available labels to plot:"
for label in $(cat "${label_file}")
do
    echo "    ${label}"

    filename="${file}.${label}"
    if [[ ( ! -f "${filename}" ) || ( "${file}" -nt "${filename}" ) ]]
    then
        grep -E "^[0-9]+ ${label} [0-9]+ [0-9]+$" "${file}" > "${filename}" &
    fi
done

wait

thread_ids="$(cat ${thread_file})"
lowest=$(echo "${thread_ids}" | head -n 1)
highest=$(echo "${thread_ids}" | tail -n 1)

echo "Thread Range: ${lowest}-${highest}"

function plot_args() {
    label="$1"
    linewidth="$2"
    shift
    shift
    title="$@"
    echo "'${file}.${label}' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth ${linewidth} title '${title}', \\"
}
