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

if [[ "$#" -lt 2 ]]
then
    echo "Syntax: $0 db-name stat [graph most recent n]"
    exit 1
fi

db="$1"
stat="$2"

if [[ "$#" -gt 2 ]]
then
    most_recent="$3"
fi

set -e

# delete existing database file
rm -f "${db}"

EXEC="gufi_query"
INDEX="example_index"

# create the database file
${SCRIPT_SOURCE}/initialize.py "${db}" "${EXEC}"

# add a configuration and get the hash
config=$(${SCRIPT_SOURCE}/configuration.py --add "${db}" --name "example config" "${EXEC}" -S "SELECT * FROM summary" -E "SELECT * FROM entries" "${INDEX}")
short="${config:0:7}"
echo "Configuration Hash (short): ${short}"

# fill the database with random numbers
${SCRIPT_SOURCE}/fill_random.py --commits 30 --runs 30 "${db}" "${config}" "${EXEC}"
# normally would run run.py:
# ${SCRIPT_SOURCE}/run.py --runs 30 --add "${db}" "${short}" gufi_query

# see change between two commits
commits=$(sqlite3 "${db}" "SELECT hash FROM commits ORDER BY id DESC LIMIT 2;")
old=$(echo "${commits}" | head -n 1)
new=$(echo "${commits}" | tail -n 1)
echo "Comparing commits ${old:0:7} and ${new:0:7}:"
${SCRIPT_SOURCE}/compare.py "${db}" "${EXEC}" "${old}" "${new}" "${stat}" \
                            opendir=-1,1     \
                            opendb=-1.5,2.5  \
                            descend=-1,1     \
                            sqlsum=-0.5,0.5  \
                            sqlent=-1.5,1.5  \
                            closedb=-2,2     \
                            closedir=-2,1    \
                            RealTime=1,2     \
                            ThreadTime=-1,1

file="${short}-${stat}"

# dump the selected stat to a file
${SCRIPT_SOURCE}/dump.py "${db}" "${EXEC}" "${config}" "${stat}" > "${file}"

# plot the stat
${SCRIPT_SOURCE}/plot.sh "${db}" "${config}" "${file}" ${most_recent}

# clean up generated files, leaving only the dump and the plot
for name in $(awk '{ print $3 }' "${file}" | sort | uniq)
do
    rm -f "${file}.${name}"
done
