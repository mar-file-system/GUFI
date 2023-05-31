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



import argparse
import os
import sqlite3
import sys

from performance_pkg import common
from performance_pkg.extraction import DebugPrints
from performance_pkg.graph import config, graph, stats
from performance_pkg.hashdb import commits as commits_table, utils as hashdb

if sys.version_info.major < 3:
    # https://stackoverflow.com/a/4935945
    # by Joe Kington
    import matplotlib as mpl
    mpl.use('Agg')

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Print stats as they are computed')
    parser.add_argument('--git-path',
                        type=os.path.abspath,
                        default='@CMAKE_SOURCE_DIR@',
                        help='Directory of repository to get commit hashes from')
    parser.add_argument('database',
                        type=str,
                        help='Hash database of configurations (must already exist)')
    parser.add_argument('raw_data_hash',
                        help='Hash of a single configuration')
    parser.add_argument('raw_data_db',
                        type=str,
                        help='Raw data database (must already exist)')
    parser.add_argument('config',
                        type=str,
                        help='Config file')

    config.override_args(parser)

    return parser.parse_args(argv)

def expand_git_identifiers(identifiers, git_path='@CMAKE_SOURCE_DIR@'):
    commits = []
    for ish in identifiers:
        freq = 1

        # try expanding identifier using git
        if '..' in ish:
            split = ish.split('%')
            if len(split) > 1:
                ish = split[0]
                freq = int(split[1])

            expanded = common.run_get_stdout(['@GIT_EXECUTABLE@', '-C', git_path, 'rev-list', ish])
        else:
            expanded = common.run_get_stdout(['@GIT_EXECUTABLE@', '-C', git_path, 'rev-parse', ish])

        # failure to expand returns either empty string or the original ish as output

        # remove last empty line from git output, reverse, and keep every freq commit
        commits += expanded.split('\n')[-2::-1][::freq]

    return commits

def gather_raw_numbers(con, table, columns, commitdata):
    '''
    Fills in commitdata[i].raw_data
    '''

    # prepare columns for use in SELECT clause
    col_str = ', '.join('"{0}"'.format(col) for col in columns)

    # get raw numbers grouped by commit
    for cd in commitdata:
        # get benchmark numbers
        get_data_sql = 'SELECT {0} FROM {1} WHERE "commit" == "{2}";'.format(
            col_str, table, cd.commit
        )
        cur = con.execute(get_data_sql)
        cd.raw_data = list(cur.fetchall())

def set_hash_len(hash, len): # pylint: disable=redefined-builtin
    if len > 0:
        return hash[:len]
    if len < 0:
        return hash[len:]
    return hash

def run(argv):
    # pylint: disable=too-many-locals
    args = parse_args(argv)

    hashdb.check_exists(args.database)
    hashdb.check_exists(args.raw_data_db)

    # process the user's config file
    conf = config.process(args.config, args)

    raw_data = conf[config.RAW_DATA]
    axes = conf[config.AXES]

    commits = expand_git_identifiers(raw_data[config.RAW_DATA_COMMITS], args.git_path)
    columns = raw_data[config.RAW_DATA_COLUMNS]
    hash_len = axes[config.AXES_X_HASH_LEN]
    plot_full_x_range = axes[config.AXES_X_FULL_RANGE]
    reorder = axes[config.AXES_X_REORDER]

    # collect data from hashes database
    debug_name = None
    ordered_commits = []
    with sqlite3.connect(args.database) as con:
        # make sure user provided configuration hash exists
        gufi_cmd, debug_name = hashdb.get_config(con, args.raw_data_hash)

        # will throw if not found
        DebugPrints.DEBUG_PRINTS[gufi_cmd][debug_name] # pylint: disable=pointless-statement

        # collect commit metadata using user inputted order
        for commit in commits:
            # get id, commit, and timestamp from the commit metadata table
            get_metadata_sql = 'SELECT * FROM {0} WHERE "commit" == "{1}";'.format(commits_table.TABLE_NAME, commit)
            cur = con.execute(get_metadata_sql)
            metadata = cur.fetchall()
            if len(metadata) != 1:
                raise ValueError('Expected 1 metadata row for commit {0}. Got {1}.'.format(commit, len(metadata)))

            idx, commit, timestamp = metadata[0]
            ordered_commits += [stats.CommitData(idx, commit, timestamp)]

    # if user requested for the commits to be sorted in history order, sort them
    # otherwise, use the user inputted order as is
    if reorder is True:
        ordered_commits.sort(key=lambda cd : cd.idx)

    # get raw numbers and put them into ordered_commits
    with sqlite3.connect(args.raw_data_db) as con:
        gather_raw_numbers(con, debug_name, columns, ordered_commits)

    # remove commits without data
    if not plot_full_x_range:
        ordered_commits = [oc for oc in ordered_commits if len(oc.raw_data) > 0]

    # generate lines from raw data
    lines = stats.generate_lines(conf, columns, ordered_commits, args.verbose)

    # plot stats and save to file
    graph.generate(conf,
                   [set_hash_len(oc.commit, hash_len)
                    for oc in ordered_commits],
                   lines)

if __name__ == '__main__':
    run(sys.argv[1:])
