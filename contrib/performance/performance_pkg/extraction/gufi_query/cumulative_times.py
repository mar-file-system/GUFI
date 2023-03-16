#!/usr/bin/env @PYTHON_INTERPRETER@
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



from performance_pkg.extraction import common

TABLE_NAME = 'cumulative_times'

# unordered cumulative times columns
# from gufi_query
COLUMN_FORMATS = [

    # commit 908c161 "reorganize gufi_query" -> Most recent commit
    # Added: attach index, xattrdone, detach index
    # Removed: open databases, sqlite3_open_v2, setpragmas, load extensions,
    #          attach intermediate databases, detach intermediate databases, close databases
    {
        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'attach index':                               float,  # New
        'xattrprep':                                  float,
        'addqueryfuncs':                              float,
        'get_rollupscore':                            float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'check if treesummary table exists':          float,
        'sqltsum':                                    float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'xattrdone':                                  float,  # New
        'detach index':                               float,  # New
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Threads run':                                  int,
        'Queries performed':                            int,
        'Rows printed to stdout or outfiles':           int,
        'Total Thread Time (not including main)':     float,
        'Real time (main)':                           float,
    },

    # commit 61c0a9d "count queries instead of multiplying" -> commit 8060d30 "split build_and_install function"
    # Added: Threads run
    # Renamed: Rows returned -> Rows printed to stdout or outfiles, Real time -> Real time (main)
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'xattrprep':                                  float,
        'get_rollupscore':                            float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'check if treesummary table exists':          float,
        'sqltsum':                                    float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Threads run':                                  int,  # New
        'Queries performed':                            int,
        'Rows printed to stdout or outfiles':           int,  # Renamed
        'Total Thread Time (not including main)':     float,
        'Real time (main)':                           float,  # Renamed
    },

    # commit 7cd35f8 "xattrprep from Gary" -> commit 4164985 "change querydb macro into a function"
    # Added: xattrprep, get_rollupscore
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'xattrprep':                                  float,  # New
        'get_rollupscore':                            float,  # New
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'check if treesummary table exists':          float,
        'sqltsum':                                    float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
        'Total Thread Time (not including main)':     float,
    },

    # commit a13a330 "gufi_query does not need a modifydb timer" -> commit 216ef5b "accidentally added argument to -w flag"
    # Removed: create tables
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'check if treesummary table exists':          float,
        'sqltsum':                                    float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
        'Total Thread Time (not including main)':     float,
    },

    # commit 75e2c5b "update tsum to use sqlite3_exec instead of rawquerydb" -> commit 00ba871 "remove --delim option from gufi_find"
    # Added check if treesummary table exists, sqltsum
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'check if treesummary table exists':          float,  # New
        'sqltsum':                                    float,  # New
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
        'Total Thread Time (not including main)':     float,
    },

    # commit 093dc32 "Added total time spent in threads to cumulative times output" -> commit "3235400 also print git branch name"
    # Added: Total Thread Time (not including main)
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
        'Total Thread Time (not including main)':     float,  # New
    },

    # commit 941e8ca "use per-executable wrappers around timestamp macros" -> 97fabf7 "dirents not of type d, f, or l are ignored"
    # Removed: clean up intermediate databases
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'sqlsum':                                     float,
        'sqlent':                                     float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
    },

    # commit aad5b08 "use struct start_end instead of individual timespecs"
    # Added: sqlsum, sqlent
    # Removed: sqlite3_exec
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'sqlsum':                                     float,  # New
        'sqlent':                                     float,  # New
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,
        'aggregate into final databases':             float,
        'clean up intermediate databases':            float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
    },

    # commit 90611bf "more DRY timestamp printing" -> commit aaa5b89 "remove travis user in docker"
    # Added: output timestamps
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'sqlite3_exec':                               float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'output timestamps':                          float,  # New
        'aggregate into final databases':             float,
        'clean up intermediate databases':            float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
    },

    # 8480cd1 "Fix tests" -> ea4c148 "gufi_stats needs to aggregate results" AND 86d3d0e "gufi_query updates" -> commit a9a1ef7 "update scripts Makefile"
    {

        'set up globals':                             float,
        'set up intermediate databases':              float,
        'thread pool':                                float,
        'open directories':                           float,
        'open databases':                             float,
        'sqlite3_open_v2':                            float,
        'create tables':                              float,
        'set pragmas':                                float,
        'load extensions':                            float,
        'addqueryfuncs':                              float,
        'descend':                                    float,
        'check args':                                 float,
        'check level':                                float,
        'check level <= max_level branch':            float,
        'while true':                                 float,
        'readdir':                                    float,
        'readdir != null branch':                     float,
        'strncmp':                                    float,
        'strncmp != . or ..':                         float,
        'snprintf':                                   float,
        'lstat':                                      float,
        'isdir':                                      float,
        'isdir branch':                               float,
        'access':                                     float,
        'set':                                        float,
        'clone':                                      float,
        'pushdir':                                    float,
        'attach intermediate databases':              float,
        'sqlite3_exec':                               float,
        'detach intermediate databases':              float,
        'close databases':                            float,
        'close directories':                          float,
        'restore timestamps':                         float,
        'free work':                                  float,
        'aggregate into final databases':             float,
        'clean up intermediate databases':            float,
        'print aggregated results':                   float,
        'clean up globals':                           float,
        'Rows returned':                                int,
        'Queries performed':                            int,
        'Real time':                                  float,
    },
]

COLUMNS = {
    # not from gufi_query
    'id':                                             None,
    'commit':                                          str,
    'branch':                                          str,
}

for column_format in COLUMN_FORMATS:
    COLUMNS.update(column_format)

def create_table(con):
    common.create_table(con, TABLE_NAME, COLUMNS)

def extract(src, commit, branch):
    return common.cumulative_times_extract(src, commit, branch, COLUMNS, COLUMN_FORMATS)

def insert(con, parsed):
    common.insert(con, parsed, TABLE_NAME, COLUMNS)
