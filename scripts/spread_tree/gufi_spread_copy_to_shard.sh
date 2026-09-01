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



if [[ "$#" -lt 6 ]]
then
    echo "Syntax: $0 extdbprefix.db prefix index entry inode mtime" 1>&2
    exit 1
fi

set -e

EXTDBPREFIX_DB="$1"   # the global external database prefix configuration file
PREFIX="$2"           # the prefix this shard db will have when it is copied to the GUFI tree
FSID="$3"             # fsid of the source filesystem
ENTRY="$4"            # path  of the specific source file whose data in the global SQLite 3 db is being extracted
INODE="$5"            # inode of the specific source file whose data in the global SQLite 3 db is being extracted
MTIME="$6"            # mtime of the specific source file whose data in the global SQLite 3 db is being extracted

# get the program for generating shard paths
# defaults to gufi_spread_shard_path.sh, but does not have to be
gen_shard_path=$(sqlite3 "${EXTDBPREFIX_DB}" "SELECT gen_shard_path FROM extdb WHERE prefix == '${PREFIX}';")
if ! command -v "${gen_shard_path}" > /dev/null 2>&1
then
    echo "Error: Failed to locate shard path generator" 1>&2
    exit 1
fi

spread_tree=$(sqlite3 "${EXTDBPREFIX_DB}" "SELECT spread_top FROM extdb WHERE prefix == '${PREFIX}';")
# not checking for existance since it might not exist yet

# generate the shard path
shard_path=$("${gen_shard_path}" "${spread_tree}" "${PREFIX}" "${FSID}" "${ENTRY}" "${INODE}" "${MTIME}")
if [[ -z "${shard_path}" ]]
then
    echo "Error: Failed to generate shard path" 1>&2
    exit 1
fi

# create the shard parent if it doesn't already exist
dir=$(dirname "${shard_path}")
mkdir -p "${dir}"

# get the program for generating a single shard database
gen_shard=$(sqlite3 "${EXTDBPREFIX_DB}" "SELECT gen_shard FROM extdb WHERE prefix == '${PREFIX}';")
if ! command -v "${gen_shard}" > /dev/null 2>&1
then
    echo "Error: Failed to locate shard generator" 1>&2
    exit 1
fi

# generate the shard database
# (run dataset-specific converter)
# previous shard database file may or may not exist; sql should drop tables if necessary
flock -x /tmp "${gen_shard}" "${shard_path}" "${FSID}" "${ENTRY}" "${INODE}" "${MTIME}"

# does not print if gen_shard failed
echo "${shard_path}"
