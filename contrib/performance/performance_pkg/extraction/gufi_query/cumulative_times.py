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

# ordered cumulative times columns
COLUMNS = [
    # not from gufi_query
    ['id',                                       None],
    ['commit',                                    str],
    ['branch',                                    str],

    # from gufi_query
    ['set up globals',                          float],
    ['set up intermediate databases',           float],
    ['thread pool',                             float],
    ['open directories',                        float],
    ['attach index',                            float],
    ['xattrprep',                               float],
    ['addqueryfuncs',                           float],
    ['get_rollupscore',                         float],
    ['descend',                                 float],
    ['check args',                              float],
    ['check level',                             float],
    ['check level <= max_level branch',         float],
    ['while true',                              float],
    ['readdir',                                 float],
    ['readdir != null branch',                  float],
    ['strncmp',                                 float],
    ['strncmp != . or ..',                      float],
    ['snprintf',                                float],
    ['lstat',                                   float],
    ['isdir',                                   float],
    ['isdir branch',                            float],
    ['access',                                  float],
    ['set',                                     float],
    ['clone',                                   float],
    ['pushdir',                                 float],
    ['check if treesummary table exists',       float],
    ['sqltsum',                                 float],
    ['sqlsum',                                  float],
    ['sqlent',                                  float],
    ['xattrdone',                               float],
    ['detach index',                            float],
    ['close directories',                       float],
    ['restore timestamps',                      float],
    ['free work',                               float],
    ['output timestamps',                       float],
    ['aggregate into final databases',          float],
    ['print aggregated results',                float],
    ['clean up globals',                        float],
    ['Threads run',                               int],
    ['Queries performed',                         int],
    ['Rows printed to stdout or outfiles',        int],
    ['Total Thread Time (not including main)',  float],
    ['Real time (main)',                        float],
]

def create_table(con):
    common.create_table(con, TABLE_NAME, COLUMNS)

def extract(src, commit, branch):
    # these aren't obtained from running gufi_query
    data = {
        'id'    : None,
        'commit': commit,
        'branch': branch,
    }

    # parse input
    for line in src:
        line = line.strip()
        if line == '':
            continue

        data.update(common.process_line(line, ':', 's'))

    # check for missing input
    for col, _ in COLUMNS:
        if col not in data:
            raise ValueError('Cumulative times data missing {0}'.format(col))

    return data

def insert(con, parsed):
    common.insert(con, parsed, TABLE_NAME, COLUMNS)
