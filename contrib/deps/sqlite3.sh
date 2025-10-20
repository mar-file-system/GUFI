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



# build and install sqlite3

set -e

PATCH_SQLITE3_OPEN="$1"

# Assume all paths exist

sqlite3_name="sqlite3"
sqlite3_prefix="${INSTALL_DIR}/${sqlite3_name}"
if [[ ! -d "${sqlite3_prefix}" ]]; then
    sqlite3_build="${BUILD_DIR}/sqlite-autoconf-3430100"
    if [[ ! -d "${sqlite3_build}" ]]; then
        sqlite3_tarball="${DOWNLOAD_DIR}/sqlite-autoconf-3430100.tar.gz"
        if [[ ! -f "${sqlite3_tarball}" ]]; then
            wget https://www.sqlite.org/2023/sqlite-autoconf-3430100.tar.gz -O "${sqlite3_tarball}"
        fi

        tar -xf "${sqlite3_tarball}" -C "${BUILD_DIR}"

        echo "Patching SQLite3 Pathname Length"
        patch -p1 -d "${sqlite3_build}" < "${SCRIPT_PATH}/sqlite-autoconf-3430100.pathname.patch"

        echo "Patching SQLite3 Max Attach Count"
        patch -p1 -d "${sqlite3_build}" < "${SCRIPT_PATH}/sqlite-autoconf-3430100.max_attach.patch"

        if [[ "${PATCH_SQLITE3_OPEN}" == "true" ]]
        then
            echo "Patching SQLite3 Open"
            patch -p1 -d "${sqlite3_build}" < "${SCRIPT_PATH}/sqlite-autoconf-3270200.open.patch"
        fi
    fi

    cd "${sqlite3_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        CFLAGS="-DSQLITE_DEFAULT_AUTOMATIC_INDEX=0 -DSQLITE_DEFAULT_AUTOVACUUM=0 -DSQLITE_DEFAULT_CACHE_SIZE=16777216 -DSQLITE_DEFAULT_LOCKING_MODE=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DSQLITE_DEFAULT_SYNCHRONOUS=0 -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=0 -DSQLITE_DQS=0 -DSQLITE_MAX_ATTACHED=254 -DSQLITE_MAX_EXPR_DEPTH=0 -USQLITE_THREADSAFE -DSQLITE_THREADSAFE=0 -DSQLITE_TEMP_STORE=3 -DSQLITE_USE_URI -USQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -USQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_MATH_FUNCTIONS -USQLITE_ENABLE_RTREE -DSQLITE_OMIT_AUTOINIT -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_JSON -DSQLITE_OMIT_PROGRESS_CALLBACK -O3" ../configure --prefix="${sqlite3_prefix}"
    fi
    make -j "${THREADS}"
    make install
fi
