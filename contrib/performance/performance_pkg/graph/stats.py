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



import numbers

import performance_pkg.graph.config

def average(data):
    return float(sum(data)) / len(data)

def median(data):
    count = len(data)
    half = int(count / 2)

    if count & 1:
        return sorted(data)[half]

    return average(sorted(data)[half - 1:half + 1])

AVERAGE = 'average'
MEDIAN  = 'median'
MINIMUM = 'minimum'
MAXIMUM = 'maximum'

STATS = {
    AVERAGE : average,
    MEDIAN  : median,
    MINIMUM : min,
    MAXIMUM : max,
}

# associate commit metadata with raw data
class CommitData: # pylint: disable=too-few-public-methods
    def __init__(self, idx, commit, timestamp):
        self.idx = idx
        self.commit = commit
        self.timestamp = timestamp
        self.raw_data = []

def single_commit_stats(columns, rows, stat_names):
    '''
    Compute stats for a single set of rows

    Parameters
    ----------
    columns : list of strings
        column names denoting what each column is
        only the length of the list is used

    rows: a list of list of numbers
        rows of data associated with the same commit

    stat_names: list of strings
        what statitics should be computed

    Returns
    -------
    dictionary of statistics for this one commit
        {statistic name (string) : statistic for each column (list)}

        e.g.:
            columns = [       'col0',       'col1',       'col2',       ... ]

            rows = [
                            [ value00,      value01,      value02,      ... ],
                            [ value10,      value11,      value12,      ... ],
                             ...
                   ]

            return
            {
                AVERAGE :   [ col0 average, col1 average, col2 average, ... ],
                MEDIAN  :   [ col0 median,  col1 median,  col2 median,  ... ],
                ...
            }
    '''
    if len(rows) == 0:
        return {stat_name: [float('nan')] * len(columns) for stat_name in stat_names}


    # make sure all values are numeric
    for row in rows:
        for col in row:
            # None values should only occur if column name does not exist at current commit
            if col is None:
                break

            if not isinstance(col, numbers.Number):
                raise TypeError('Raw value {0} is not numeric'.format(col))

    stats = {stat_name : [] for stat_name in stat_names}
    for c in range(len(columns)):
        col_data = [row[c] for row in rows]
        for stat_name in stat_names:
            # If first entry is None, all will be
            if col_data[0] is None:
                stats[stat_name] += [float('nan')]
            else:
                stats[stat_name] += [STATS[stat_name](col_data)]

    return stats

def multiple_commit_stats(columns, commitdata, stat_names, verbose):
    '''
    loop through each commit's rows and compute stats for each column

    Parameters
    ----------
    columns: list of strings
        column names denoting what each column of data is
        only the length of the list is used

    commitdata: list of CommitData
        contains raw benchmark data

    stat_names: list of strings
        what statitics should be computed

    verbose: bool
        whether or not to print out stats as they are calculated

    Returns
    -------
    list of dictionary of stats in the same order as the commits/raw data

        e.g.:
        [
            result from single_commit_stats,
            ...
        ]
    '''

    col_count = len(columns)
    stats = []
    for cd in commitdata:
        scs = single_commit_stats(columns, cd.raw_data, stat_names)
        stats += [scs]

        if verbose:
            for col in range(col_count):
                print('Commit {0}: Col: "{1}\": Rows: {2}, {3}'.format(
                    cd.commit, columns[col], len(cd.raw_data),
                    ', '.join(['{0}: {1}'.format(stat_name, scs[stat_name][col])
                               for stat_name in stat_names])
            ))

    return stats

def generate_lines(conf, columns, commitdata, verbose):
    '''
    Function for encapsulating raw data processing in preparation for graphing

    Parameters
    ----------
    conf: dictionary
        configuration provided by user

    columns: list of strings
        column names denoting what each column of data is
        only the length of the list is used

    commitdata: list of CommitData
        contains raw benchmark data

    verbose: bool
        whether or not to print out stats as they are calculated

    Returns
    -------
    dictionary of dictionary of list of stats across commits

        e.g.
        {
            'col0' : {
                         AVERAGE : [commit0 average, commit1 average, ...],
                         MEDIAN  : [commit0 median,  commit1 median,  ...],
                         ...
                     },
            'col1' : {
                         AVERAGE : [commit0 average, commit1 average, ...],
                         MEDIAN  : [commit0 median,  commit1 median,  ...],
                         ...
                     },
            ...
        }
    '''

    # statistic names come from axes and error bar
    axes = conf[performance_pkg.graph.config.AXES]
    error_bar = conf[performance_pkg.graph.config.ERROR_BAR]

    # get unique set of statistics to compute
    stat_names = []
    for stat_name in list({axes[performance_pkg.graph.config.AXES_Y_STAT],
                           error_bar[performance_pkg.graph.config.ERROR_BAR_BOTTOM],
                           error_bar[performance_pkg.graph.config.ERROR_BAR_TOP]}):
        if stat_name is None:
            continue

        if stat_name not in STATS:
            raise ValueError('Invalid statistic name: {0}'.format(stat_name))

        stat_names += [stat_name]

    stats = multiple_commit_stats(columns, commitdata, stat_names, verbose)

    # for each line, generate lists for each statistic
    return {columns[c] : {stat_name : [commit_stats[stat_name][c]
                                       for commit_stats in stats]
                          for stat_name in stat_names}
            for c in range(len(columns))}
