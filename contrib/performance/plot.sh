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



SCRIPT_SOURCE="$(dirname $(realpath ${BASH_SOURCE[0]}))"

if [[ "$#" -lt 3 ]]
then
    echo "Syntax: $0 db config dump [most recent n]"
    exit 1
fi

db="$1"
config="$2"
file="$3"

config_hash=$(sqlite3 "${db}" "SELECT hash FROM configurations WHERE hash LIKE '${config}%'")

if [[ -z "${config_hash}" ]]
then
    echo "Could not find configuration hash ${config}"
    exit 2
fi

if [[ "$(echo ${config_hash} | wc -l)" -gt "1" ]]
then
    echo "Use a longer hash. Multiple configurations matched ${config}:\n${config_hash}"
    exit 3
fi

# no need to filter stats by configuration hash because that was done by dump.py

# change x-axis starting index
# do this here to get sqlite3 calls out of the way
xrange=""
if [[ "$#" -gt 3 ]]
then
    most_recent="$4"
    count=$(awk '{ print $1 }' "${file}" | sort | uniq | wc -l)

    if [[ "${most_recent}" -lt "${count}" ]]
    then
        xrange="set xrange [$(( ${count}-${most_recent} )):]"
    fi
fi

# split dump by event name
names=$(awk '{ print $3 }' "${file}" | sort | uniq)

for name in ${names}
do
    grep -E "^[0-9]+ .* ${name} [0-9]+" "${file}" > "${file}.${name}" &
done
wait

gnuplot <<EOF &

set terminal svg size 1920,1024 noenhanced fixed mouse standalone solid linewidth 2
set output "${file}.svg"
set title "gufi_query with configuration ${config_hash:0:7}"
set xlabel "Commit Hash"
set ylabel "Time"
set key outside right
set xtics rotate by -45
${xrange}

hash(col) = substr(strcol(col), 0, 7)

info(hash_col, duration) = sprintf("%s\n%s",   \
                               hash(hash_col), \
                               strcol(duration))

plot '${file}.opendir'  using 1:4:xticlabels(hash(2)) with lines title 'opendir',   \
     '${file}.opendb'   using 1:4:xticlabels(hash(2)) with lines title 'opendb',    \
     '${file}.descend'  using 1:4:xticlabels(hash(2)) with lines title 'descend',   \
     '${file}.sqlsum'   using 1:4:xticlabels(hash(2)) with lines title 'sqlsum',    \
     '${file}.sqlent'   using 1:4:xticlabels(hash(2)) with lines title 'sqlent',    \
     '${file}.closedb'  using 1:4:xticlabels(hash(2)) with lines title 'closedb',   \
     '${file}.closedir' using 1:4:xticlabels(hash(2)) with lines title 'closedir',  \
     '${file}.RealTime' using 1:4:xticlabels(hash(2)) with lines title 'Real Time', \
     '${file}.opendir'  using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.opendb'   using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.descend'  using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.sqlsum'   using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.sqlent'   using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.closedb'  using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.closedir' using 1:4:(info(2, 4)) with labels hypertext notitle,       \
     '${file}.RealTime' using 1:4:(info(2, 4)) with labels hypertext notitle,       \

EOF

wait
