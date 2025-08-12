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

threads=1

# https://stackoverflow.com/a/14203146
# Bruno Bronosky
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --threads|-n)
        threads="$2"
        shift # past count
        ;;
    *)    # unknown option
        POSITIONAL+=("$1") # save it in an array for later
        ;;
esac
    shift # past flag
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ "$#" -lt 2 ]]; then
    echo "Syntax: $0 [--threads|-n <#>] index outdb"
    exit 1
fi

index="$1"
outdb="$2"

rm -f "${outdb}"

# one-pass collection of stats grouped by level
# writing to database file ${outdb}
# assuming not rolled up
gufi_query \
    -d " " \
    -n "${threads}" \
    -O "${outdb}" \
    -I "CREATE TABLE intermediate (level INT64, size INT64, atime INT64, mtime INT64, ctime INT64, totfiles INT64, totlinks INT64, totsubdirs INT64);" \
    -E "INSERT INTO intermediate SELECT level() AS level, dtotsize, CAST(TOTAL(atime) AS INT64), CAST(TOTAL(mtime) AS INT64), CAST(TOTAL(ctime) AS INT64), dtotfiles, dtotlinks, subdirs(srollsubdirs, sroll) FROM vrpentries;" \
    -K "CREATE TABLE aggregate (level INT64, size INT64, atime INT64, mtime INT64, ctime INT64, totdirs INT64, totfiles INT64, totlinks INT64, totsubdirs INT64); CREATE TABLE stats (level INT64, totdirs INT64, totsubdirs INT64, mean_subdirs INT64, totfiles INT64, totlinks INT64, totsize INT64, mean_dir_size INT64, mean_file_size INT64, mean_atime INT64, mean_mtime INT64, mean_ctime INT64)" \
    -J "INSERT INTO aggregate SELECT level, SUM(size), SUM(atime), SUM(mtime), SUM(ctime), COUNT(*), SUM(totfiles), SUM(totlinks), SUM(totsubdirs) FROM intermediate GROUP BY level" \
    -G "INSERT INTO stats SELECT level, SUM(totdirs), SUM(totsubdirs), COALESCE(SUM(totsubdirs) / SUM(totdirs), 0), COALESCE(SUM(totfiles), 0), COALESCE(SUM(totlinks), 0), COALESCE(SUM(size), 0), COALESCE(SUM(size) / SUM(totdirs), 0), COALESCE(SUM(size) / SUM(totfiles), 0), COALESCE(SUM(atime) / (SUM(totfiles) + SUM(totlinks)), 0), COALESCE(SUM(mtime) / (SUM(totfiles) + SUM(totlinks)), 0), COALESCE(SUM(ctime) / (SUM(totfiles) + SUM(totlinks)), 0) FROM aggregate GROUP BY level; DROP TABLE aggregate;" \
    "${index}"
