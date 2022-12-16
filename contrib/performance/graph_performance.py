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
import numbers
import sqlite3
import sys

if sys.version_info.major < 3:
    import matplotlib as mpl
    mpl.use('Agg')

# pylint: disable=wrong-import-position
from performance_pkg.graph import config, graph
from performance_pkg.extraction.gufi_query import cumulative_times as gq_cumulative_times
from performance_pkg.extraction.gufi_trace2index import cumulative_times as gt_cumulative_times
from performance_pkg.hashdb import gufi, utils as hashdb

def parse_args(argv):
    parser = argparse.ArgumentParser()
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
    return parser.parse_args(argv)

def gather_raw_numbers(dbname, table_name, columns, commits):
    raw_numbers = [] # raw numbers grouped by commit

    # extract raw values from database
    hashdb.check_exists(dbname)
    try:
        con = sqlite3.connect(dbname)

        # convert columns into SELECT clause
        col_str = ', '.join('"{0}"'.format(col) for col in columns)

        # get raw numbers grouped by commit
        for commit in commits:
            query = 'SELECT {0} FROM {1} WHERE "commit" == "{2}";'.format(
                col_str, table_name, commit
            )
            cur = con.execute(query)
            raw_numbers += [cur.fetchall()]
    finally:
        con.close()

    return raw_numbers

class CommitStats(object): # pylint: disable=useless-object-inheritance, too-few-public-methods
    def __init__(self, commit, rows, col_names):
        # make sure all values are numeric
        for row in rows:
            for col in row:
                if not isinstance(col, numbers.Number):
                    raise TypeError('Raw value {0} is not numeric'.format(col))

        # no need to save raw values
        self.commit = commit

        # values to plot
        self.average = []
        self.median  = []
        self.minimum = []
        self.maximum = []

        self.row_count = len(rows)
        self.col_count = len(col_names)

        if self.row_count > 0:
            for col in range(self.col_count):
                col_data = [row[col] for row in rows]

                self.average += [float(sum(col_data)) / self.row_count]
                half = int(self.row_count / 2)
                if self.row_count & 1:
                    self.median += [sorted(col_data)[half]]
                else:
                    self.median += [sum(sorted(col_data)[half - 1:half + 1]) / 2]
                self.minimum += [min(col_data)]
                self.maximum += [max(col_data)]
        else:
            self.average = [float('nan')] * self.col_count
            self.median  = [float('nan')] * self.col_count
            self.minimum = [float('nan')] * self.col_count
            self.maximum = [float('nan')] * self.col_count

def compute_stats(commits, raw_data, columns):
    # calculate stats by commit
    stats = []
    for commit, raw in zip(commits, raw_data):
        # calculate stats for this commit
        stats += [CommitStats(commit, raw, columns)]
    return stats

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
    table_name = None
    gufi_cmd, debug_name = hashdb.get_config(args.database, args.raw_data_hash)

    if debug_name == gufi.CUMULATIVE_TIMES:
        if gufi_cmd == gufi.GUFI_QUERY:
            table_name = gq_cumulative_times.TABLE_NAME
        if gufi_cmd == gufi.GUFI_TRACE2INDEX:
            table_name = gt_cumulative_times.TABLE_NAME

    if table_name is None:
        raise ValueError('Configuration Hash "{0}" not found.'.format(args.raw_data_hash))

    # process the user's config file
    conf = config.process(args.config)

    # aliases
    raw_data = conf[config.RAW_DATA]
    axes = conf[config.AXES]
    commits = raw_data[config.RAW_DATA_COMMITS]
    columns = raw_data[config.RAW_DATA_COLUMNS]

    # get raw data
    raw_numbers = gather_raw_numbers(args.raw_data_db, table_name, columns, commits)

    # compute stats
    stats = compute_stats(commits, raw_numbers, columns)

    # reorganize stats into lines
    averages  = []
    medians   = []
    minimums  = []
    maximums  = []
    low_pnts  = []
    high_pnts = []
    for col in range(len(columns)):
        # extract a single column's stats
        averages  += [[commit.average[col] for commit in stats]]
        medians   += [[commit.median[col]  for commit in stats]]
        minimums  += [[commit.minimum[col] for commit in stats]]
        maximums  += [[commit.maximum[col] for commit in stats]]
        low_pnts  += [[commit.average[col] - commit.minimum[col] for commit in stats]]
        high_pnts += [[commit.maximum[col] - commit.average[col] for commit in stats]]

    # modify the commit list to length specified in the config
    commits = [set_hash_len(commit, axes[config.AXES_HASH_LEN])
               for commit in commits]
    # plot stats
    graph.generate(conf, commits, averages, columns, low_pnts, high_pnts, minimums, maximums)

    return 0

if __name__ == '__main__':
    sys.exit(run(sys.argv[1:]))
