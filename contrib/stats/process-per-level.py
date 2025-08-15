#!@Python3_EXECUTABLE@
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

def l2(lhs, rhs):
    return sum([(pair[0] - pair[1]) ** 2 for pair in zip(lhs, rhs)]) ** 0.5

def cosine(lhs, rhs):
    top = sum([(pair[0] * pair[1]) for pair in zip(lhs, rhs)])
    bot = sum([val ** 2 for val in lhs]) ** 0.5 * sum([val ** 2 for val in rhs]) ** 0.5
    return top / bot

COMPARE = {
    'l2'     : l2,      # distance
    'cosine' : cosine,  # magnitude
}

def parse_args():
    parser = argparse.ArgumentParser('Compare GUFI tree stats')

    parser.add_argument('--table',
                        type=str,
                        default='stats')

    parser.add_argument('lhs',
                        type=str,
                        help='db file containing stats for the left hand side of the comparison')

    parser.add_argument('rhs',
                        type=str,
                        help='db file containing stats for the right hand side of the comparison')

    parser.add_argument('algorithm',
                        choices=COMPARE.keys())

    parser.add_argument('column',
                        type=str, # do not attempt to parse user input to exclude level
                        nargs='*',
                        help='column names (do not include level)')

    return parser.parse_args()

# pull columns from stat db
# level should always be the first column
def extract_stats(filename, columns, table):
    if columns == []:
        columns = ['*']
    else:
        columns = ['level'] + columns

    with sqlite3.connect(filename) as conn:
        return conn.execute('SELECT {0} FROM {1};'.format(','.join(columns), table)).fetchall()

# both sets of stats should have the same number of levels
def pad_missing_levels(lhs, rhs):
    # both stats should have the same number of columns
    col_count = len(lhs[0])

    # add missing upper levels
    # not setting first column (level) because it won't be used
    lhs_first_level = lhs[0][0]
    lhs = [[0] * col_count for _ in range(lhs_first_level)] + lhs
    rhs_first_level = rhs[0][0]
    rhs = [[0] * col_count for _ in range(rhs_first_level)] + rhs

    # fill missing lower levels with 0s
    # not setting first column (level) because it won't be used
    lhs_depth = len(lhs)
    rhs_depth = len(rhs)
    if lhs_depth < rhs_depth:
        lhs += [[0] * col_count] * (rhs_depth - lhs_depth)
    elif lhs_depth > rhs_depth:
        rhs += [[0] * col_count] * (lhs_depth - rhs_depth)

    return lhs, rhs

# convert a set of stats to a vector
# in this case, concatenate each column into one giant column
def stats_to_vector(stats):
    col_count = len(stats[0])

    vector = []
    for col in range(1, col_count): # start at 1 to skip level column
        vector += [row[col] for row in stats]

    return vector

def main():
    args = parse_args()

    lhs = extract_stats(args.lhs, args.column, args.table)
    rhs = extract_stats(args.rhs, args.column, args.table)

    lhs, rhs = pad_missing_levels(lhs, rhs)

    lhs_vec = stats_to_vector(lhs)
    rhs_vec = stats_to_vector(rhs)

    if len(lhs_vec) != len(rhs_vec):
        raise RuntimeError('Not comparing vectors of different lengths: {0} vs {1}'.format(
            len(lhs_vec), len(rhs_vec)))

    print(COMPARE[args.algorithm](lhs_vec, rhs_vec))

if __name__ == '__main__':
    main()
