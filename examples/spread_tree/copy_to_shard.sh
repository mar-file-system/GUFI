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



# example using an filesystem entry to pull data from some source
# (actual source filesystem, extracted data file, etc.) and place that
# data into a spread tree shard database file.
#
# In this example, the data has already been extracted and placed into
# a global SQLite 3 database file, so this script does not do any
# extraction, and copies directly from it to the shard, which is also
# a SQLite 3 database file.

DIR=$(dirname "${BASH_SOURCE[0]}")

# all extraction scripts should have these arguments
if [[ "$#" -lt 5 ]]
then
    echo "Syntax: $0 shard fsid entry inode mtime" 1>&2
    exit 1
fi

set -e

SHARD="$1"    # path to the shard database where data should be copied to
# FSID="$2"     # fsid of the filesystem
# ENTRY="$3"    # path of the specific source entry whose data in the global sqlite3 db is being extracted
INODE="$4"    # inode of the specific source entry whose data in the global sqlite3 db is being extracted
MTIME="$5"    # mtime of the specific source entry whose data in the global sqlite3 db is being extracted

# ############################

# constants for this specific dataset + prefix
# possibly source these variables instead of having them here

SCHEMA="${DIR}/schema.sql"     # schema of the shard database
PROCESSED_DATA="extracted.db"  # location of global extracted data file
SRC_TABLE_NAME="extracted"     # name of table in extracted.db
DST_TABLE_NAME="extracted"     # name of shard table

# data has already been extracted, so no need to do it here

# create the shard db
(
    # attach source data (source is also a SQLite 3 database)
    ATTACH_NAME="contents"     # attach extracted.db with this name
    echo "ATTACH 'file:${PROCESSED_DATA}?mode=ro' AS ${ATTACH_NAME};"

    # set up tables
    cat "${SCHEMA}"

    # copy extracted data to the shard
    echo "INSERT INTO ${DST_TABLE_NAME} "
    echo "SELECT inode, mtime, data "
    echo "FROM ${ATTACH_NAME}.${SRC_TABLE_NAME} "
    echo "WHERE (inode == '${INODE}') AND (mtime == ${MTIME});"

    # clean up
    echo "DETACH ${ATTACH_NAME};" # not strictly necessary
) | sqlite3 "${SHARD}"
# ############################
