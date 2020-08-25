#!/usr/bin/env python2
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
import sqlite3

import available
import stats

# Convert strings of the form
#
#     name=float,float
#
# into elements of a list
#
# The new value is compared against
# an open interval created by the
# two floats added to the old value:
#
#     (old + lower, old + higher)
#
def selected_type(column):
    name, values  = column.split('=')
    lower, higher = values.split(',')
    return [name, [float(lower), float(higher)]]

def add_args(parser, optional = False):
    parser.add_argument('column', nargs='*' if optional else '+', type=selected_type,
                        action='append', default=[],
                        help='Columns to compare (format: <column name>=<float>,<float>)')
    return parser

def get_stat_row(cursor, selected, stat, commit):
    if len(selected) == 0:
        return []

    cursor.execute('''SELECT {} FROM {} WHERE git LIKE '{}%';'''.format(','.join([col[0] for col in selected]), stat, commit))
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise RuntimeError('''Got bad number of rows from {} for hash '{}': {}'''.format(stat, commit, len(rows)))

    return rows[0]

# get data in format for easy comparison
# variance is a sorted array that defines the range the new data
# is allowed be within to be considered the same as the old data
# it is offsets from the old data, not absolute values
#
# selected, old_data, and new_data should be
# in the same order for this to make sense
def get_raw(selected,
            old_data,
            new_data,
            stat):
    results = []
    for i in xrange(len(selected)):
        name, variance = selected[i]
        results += [(name, old_data[i], new_data[i], variance)]
    return results

def compare(cursor,
            stat, selected,
            old_hash, new_hash,
            old_stat_table, new_stat_table):
    # get the data of the hashes
    old_data = get_stat_row(cursor, selected, old_stat_table, old_hash)
    new_data = get_stat_row(cursor, selected, new_stat_table, new_hash)

    # get tuples for comparison
    raw = get_raw(selected, old_data, new_data, stat)

    # compare each column's statistic and print results
    for name, old, new, variance in raw:
        lower  = old + variance[0]
        higher = old + variance[1]

        change = None
        if new < lower:
            change = 'decreased'
        elif new > higher:
            change = 'increased'
        else:
            change = 'same'

        print '{} {} {} {}'.format(name, old, new, change)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='GUFI performance comparison')
    parser.add_argument('database', type=str,
                        help='Database of records')
    available.add_choices(parser, False)
    parser.add_argument('commit', nargs=2, type=str,
                        help='Commits to compare')
    parser.add_argument('stat', choices=[view.name for view in stats.VIEWS],
                        help='Which statistic to compare')

    add_args(parser)

    args = parser.parse_args()

    db = sqlite3.connect(args.database)
    cursor = db.cursor()

    old_hash, new_hash = args.commit

    compare(cursor,
            args.stat, args.column[0],
            old_hash, new_hash,
            args.stat, args.stat)

    cursor.close()
