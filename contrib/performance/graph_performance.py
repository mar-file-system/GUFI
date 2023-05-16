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
from performance_pkg.hashdb import utils as hashdb

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

def gather_raw_numbers(dbname, table_name, columns, commits, plot_full_x_range):
    raw_numbers = [] # raw numbers grouped by commit

    # extract raw values from database
    hashdb.check_exists(dbname)
    with sqlite3.connect(dbname) as con:
        # convert columns into SELECT clause
        col_str = ', '.join('"{0}"'.format(col) for col in columns)

        # get raw numbers grouped by commit
        for commit in commits[:]:
            query = 'SELECT {0} FROM {1} WHERE "commit" == "{2}";'.format(
                col_str, table_name, commit
            )
            cur = con.execute(query)
            raw_commit = cur.fetchall()

            # Skip empty commits if not plotting the full range
            if len(raw_commit) == 0:
                if not plot_full_x_range:
                    commits.remove(commit)
                    continue

            raw_numbers += [raw_commit]

    return raw_numbers

def set_hash_len(hash, len): # pylint: disable=redefined-builtin
    if len > 0:
        return hash[:len]
    if len < 0:
        return hash[len:]
    return hash

def run(argv):
    # pylint: disable=too-many-locals
    args = parse_args(argv)

    # get table name where raw data is stored
    gufi_cmd, debug_name = hashdb.get_config(args.database, args.raw_data_hash)

    # will throw if not found
    debug_print = DebugPrints.DEBUG_PRINTS[gufi_cmd][debug_name]

    # process the user's config file
    conf = config.process(args.config, args)

    # aliases
    raw_data = conf[config.RAW_DATA]
    commits = expand_git_identifiers(raw_data[config.RAW_DATA_COMMITS], args.git_path)
    columns = raw_data[config.RAW_DATA_COLUMNS]
    plot_full_x_range = conf[config.AXES][config.AXES_X_FULL_RANGE]

    # get raw data
    raw_numbers = gather_raw_numbers(args.raw_data_db, debug_print.TABLE_NAME,
                                     columns, commits, plot_full_x_range)

    # generate lines from raw data
    lines = stats.generate_lines(conf, commits, columns, raw_numbers, args.verbose)

    # modify the commit list to length specified in the config
    hash_len = conf[config.AXES][config.AXES_X_HASH_LEN]
    commits = [set_hash_len(commit, hash_len)
               for commit in commits]

    # plot stats and save to file
    graph.generate(conf, commits, lines)

if __name__ == '__main__':
    run(sys.argv[1:])
