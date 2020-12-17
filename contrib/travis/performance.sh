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
set -x

# start at contrib directory
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
ROOT="$(realpath ${SCRIPT_PATH}/../..)"

export PATH="${ROOT}/build/src:${ROOT}/build/scripts:${PATH}"

CPUS="$(nproc --all)"

# index the entire repository
INDEX="/tmp/index"
gufi_dir2index -n "${CPUS}" "${ROOT}" "${INDEX}"

cd "${ROOT}/contrib/performance"

# run benchmarks on each executable
for EXEC in "${ROOT}/build/src/gufi_query"
do
    name="$(basename ${EXEC})"
    DB="${name}.db"

    # set up the database
    if [[ ! -f "${DB}" ]]
    then
        ./initialize.py "${DB}" "${EXEC}"
    fi

    CONFIG_HASH=$(./configuration.py --name "Travis ${TRAVIS_OS_NAME}"       \
                                     --cpu "${TRAVIS_CPU_ARCH}"              \
                                     --memory "7.5 GB"                       \
                                     "${name}"                               \
                                     --threads "${CPUS}"                     \
                                     --summary "SELECT * FROM summary"       \
                                     --entries "SELECT * FROM entries"       \
                                     --buffer_size 1024 --terse "${INDEX}"   \
                                     --add "${DB}")

    sudo ./run.py --add "${DB}" "${CONFIG_HASH}" --executable-path="${EXEC}" \
         --stat average "${name}"                                            \
         setup_globals=-1,1                                                  \
         setup_aggregate=-1,1                                                \
         work=-1,1                                                           \
         opendir=-1,1                                                        \
         opendb=-1,1                                                         \
         sqlite3_open=-1,1                                                   \
         set_pragmas=-1,1                                                    \
         load_extension=-1,1                                                 \
         addqueryfuncs=-1,1                                                  \
         descend=-1,1                                                        \
         check_args=-1,1                                                     \
         level=-1,1                                                          \
         level_branch=-1,1                                                   \
         while_branch=-1,1                                                   \
         readdir=-1,1                                                        \
         readdir_branch=-1,1                                                 \
         strncmp=-1,1                                                        \
         strncmp_branch=-1,1                                                 \
         snprintf=-1,1                                                       \
         lstat=-1,1                                                          \
         isdir=-1,1                                                          \
         isdir_branch=-1,1                                                   \
         access=-1,1                                                         \
         set_struct=-1,1                                                     \
         clone=-1,1                                                          \
         pushdir=-1,1                                                        \
         attach=-1,1                                                         \
         sqltsumcheck=-1,1                                                   \
         sqltsum=-1,1                                                        \
         sqlsum=-1,1                                                         \
         sqlent=-1,1                                                         \
         detach=-1,1                                                         \
         closedb=-1,1                                                        \
         closedir=-1,1                                                       \
         utime=-1,1                                                          \
         free_work=-1,1                                                      \
         output_timestamps=-1,1                                              \
         aggregate=-1,1                                                      \
         output=-1,1                                                         \
         cleanup_globals=-1,1                                                \
         rows=-1,1                                                           \
         query_count=-1,1                                                    \
         RealTime=-1,1                                                       \
         ThreadTime=-1,1

    ./dump.py "${DB}" "${EXEC}" "${CONFIG_HASH}" "${STAT}" > "${EXEC}.${STAT}"

    ./plot.sh "${DB}" "${CONFIG_HASH}" "${EXEC}.${STAT}"
done
