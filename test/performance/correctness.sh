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

DB="test_perf.db"
OUTPUT="correctness.out"

function cleanup() {
    rm -f "${DB}"
}

trap cleanup EXIT

cleanup

(
echo "Initialize database file ${DB}"
${ROOT}/contrib/performance/initialize.py "${DB}" gufi_query > /dev/null
echo

echo "Tables:"
sqlite3 "${DB}" "SELECT '    ' || name FROM sqlite_master WHERE type=='table';" | sort
echo

echo "Views:"
sqlite3 "${DB}" "SELECT '    ' || name FROM sqlite_master WHERE type=='view';"  | sort
echo

echo "Create a configuration"
config_hash=$(${ROOT}/contrib/performance/configuration.py --add "${DB}" gufi_query "testindex.gufi")
echo "    Hash: ${config_hash}"
echo "Number of configurations available:" $(sqlite3 "${DB}" "SELECT COUNT(*) FROM configurations")
echo

echo "Insert raw numbers"
declare -a commits=("commit1" "commit2")
branch="test"

for run in $(seq 1 5)
do
    # only RealTime has values - all columns should be calculated the same way
    sqlite3 "${DB}" "INSERT INTO raw_numbers VALUES ('${config_hash}', '${commits[0]}', '${branch}', ${run}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ${run}, 0);"

    sqlite3 "${DB}" "INSERT OR IGNORE INTO commits VALUES (NULL, '${commits[0]}');"
done

for run in $(seq 2 6)
do
    # only RealTime has values - all columns should be calculated the same way
    sqlite3 "${DB}" "INSERT INTO raw_numbers VALUES ('${config_hash}', '${commits[1]}', '${branch}', ${run}, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ${run}, 0);"

    sqlite3 "${DB}" "INSERT OR IGNORE INTO commits VALUES (NULL, '${commits[1]}');"
done

# there should be 5 runs for 2 commits = 10 rows of raw numbers
echo "Sets of raw numbers:" $(sqlite3 "${DB}" "SELECT COUNT(*) FROM raw_numbers;")
echo

# 2 rows - one for each commit
echo "Rows in average:" $(sqlite3 "${DB}" "SELECT COUNT(*) FROM average;")
echo "Rows in max:    " $(sqlite3 "${DB}" "SELECT COUNT(*) FROM max;")
echo "Rows in min:    " $(sqlite3 "${DB}" "SELECT COUNT(*) FROM min;")
echo

for commit in "${commits[@]}"
do
    echo "${commit} average:" $(sqlite3 "${DB}" "SELECT RealTime FROM average WHERE git=='${commit}';")
    echo "${commit} max:    " $(sqlite3 "${DB}" "SELECT RealTime FROM max     WHERE git=='${commit}';")
    echo "${commit} min:    " $(sqlite3 "${DB}" "SELECT RealTime FROM min     WHERE git=='${commit}';")
    echo
done

# test comparisons
echo "Compare average"
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" average RealTime=-0.5,0.5
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" average RealTime=-1,1
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" average RealTime=2,3
echo

echo "Compare max"
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" max RealTime=-0.5,0.5
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" max RealTime=-1,1
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" max RealTime=2,3
echo

echo "Compare min"
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" min RealTime=-0.5,0.5
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" min RealTime=-1,1
${ROOT}/contrib/performance/compare.py "${DB}" gufi_query "${commits[0]}" "${commits[1]}" min RealTime=2,3
echo

echo "Dump average"
${ROOT}/contrib/performance/dump.py "${DB}" gufi_query "${config_hash}" average
echo

echo "Dump max"
${ROOT}/contrib/performance/dump.py "${DB}" gufi_query "${config_hash}" max
echo

echo "Dump min"
${ROOT}/contrib/performance/dump.py "${DB}" gufi_query "${config_hash}" min
echo
) 2>&1 | tee "${OUTPUT}"

diff ${ROOT}/test/performance/correctness.expected "${OUTPUT}"
rm "${OUTPUT}"
