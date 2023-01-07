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

PERFORMANCE_PATH="@CMAKE_BINARY_DIR@/contrib/performance"

export PATH="${PERFORMANCE_PATH}:@DEP_INSTALL_PREFIX@/sqlite3/bin:${PATH}"
export LD_LIBRARY_PATH="@DEP_INSTALL_PREFIX@/sqlite3/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="@CMAKE_BINARY_DIR@/contrib:${PERFORMANCE_PATH}:@CMAKE_BINARY_DIR@/scripts:${PYTHONPATH}"

# database containing hashes of machine properties and gufi commands
HASHDB="hashes.db"

# database containing raw data (gufi_query + cumulative times)
RAW_DATA_DB="example_raw_data.db"

# config file describing what to graph
CONFIG_FILE="${PERFORMANCE_PATH}/configs/db_query.ini"

GUFI_CMD="gufi_query"
DEBUG_NAME="cumulative-times"
TREE="test_tree"
OVERRIDE="custom_hash_value"

cleanup() {
    rm -f "${HASHDB}" "${RAW_DATA_DB}"

    # delete generated graph
    rm -f "$(grep path ${CONFIG_FILE} | awk '{ print $3 }')"
}

cleanup

# setup hash database and insert hashes
"${PERFORMANCE_PATH}/setup_hashdb.py" "${HASHDB}"
machine_hash=$("${PERFORMANCE_PATH}/machine_hash.py" --database "${HASHDB}")
gufi_hash=$("${PERFORMANCE_PATH}/gufi_hash.py" --database "${HASHDB}" "${GUFI_CMD}" "${DEBUG_NAME}" "${TREE}")
raw_data_hash=$("${PERFORMANCE_PATH}/raw_data_hash.py" --database "${HASHDB}" --override "${OVERRIDE}" "${machine_hash}" "${gufi_hash}")

# setup raw data database
"${PERFORMANCE_PATH}/setup_raw_data_db.py" "${HASHDB}" "${raw_data_hash}" "${RAW_DATA_DB}"

for((commit=0; commit<5; commit++))
do
    for((x=0; x<5; x++))
    do
        # drop caches

        # "query" the build directory
        "@CMAKE_BINARY_DIR@/src/gufi_query" -E "SELECT * FROM pentries" "@CMAKE_BINARY_DIR@" 2>&1 > /dev/null | "${PERFORMANCE_PATH}/extract.py" "${HASHDB}" --commit "commit-${commit}" --branch "example" "${raw_data_hash}" "${RAW_DATA_DB}"
    done
done

"${PERFORMANCE_PATH}/graph_performance.py" "${HASHDB}" "${raw_data_hash}" "${RAW_DATA_DB}" "${CONFIG_FILE}"
