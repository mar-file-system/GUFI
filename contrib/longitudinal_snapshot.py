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
import numpy as np
import pandas as pd
import sqlite3
import subprocess
import time

from gufi_common import build_query, get_char, get_positive, VRSUMMARY, VRPENTRIES, VRXPENTRIES
import gufi_config

METADATA_TABLE_NAME = 'metadata'
SNAPSHOT_VIEW = 'snapshot'
PINODE = 'pinode'

# returns a list of lists of strings
# pylint: disable=too-many-arguments
def run_query(config, delim,
              columns, tables, where=None, group_by=None, order_by=None, num_results=None,
              xattrs=False):
    # keep this here to make debugging easier
    query = build_query(columns, tables, where, group_by, order_by, num_results)

    cmd = [
        config.query(),
        config.indexroot(),
        '-n', str(config.threads()),
        '-d', delim,
        '-E', query,
    ]

    if xattrs is True:
        cmd += ['-x']

    query = subprocess.Popen(cmd, stdout=subprocess.PIPE)     # pylint: disable=consider-using-with
    out, err = query.communicate()                            # block until query finishes

    return [row.split(delim) for row in out.decode('utf-8').split('\n')[:-1]]

# convert a list of lists of strings to a dataframe
def to_df(data, col_names):
    if len(data) == 0:
        return pd.DataFrame(columns=col_names)
    return pd.DataFrame(data, columns=col_names)

# get columns directly out of summary table
def get_summary_data(conn, config, delim):
    TABLE_NAME = 'summary'
    SUMMARY_COLS = [
        'name', 'inode', 'mode', 'nlink', 'uid', 'gid',
        'blksize', 'blocks', 'atime', 'mtime', 'ctime',
        'linkname', 'level()',
        'NULL', #filesystem_type
        'pinode',
        'totfiles', 'totlinks', 'subdirs(srollsubdirs, sroll)',
    ]

    df_summary_cols = SUMMARY_COLS.copy()
    df_summary_cols[-6] = 'depth'
    df_summary_cols[-1] = 'totsubdirs'
    df_summary_cols[-5] = 'filesystem_type'

    to_df(run_query(config, delim, SUMMARY_COLS, [VRSUMMARY]),
          df_summary_cols).to_sql(TABLE_NAME, conn, index=False, if_exists='fail')

    return TABLE_NAME, df_summary_cols

# returns table with pinode, mode, mode_count
def mode(config, delim, col):
    select_cols = [PINODE, col, 'COUNT({0}) AS count'.format(col)]
    col_names = [PINODE, 'mode', 'mode_count']
    return to_df(run_query(config, delim,
                           select_cols, [VRPENTRIES], None, [col], ['count DESC'], 1),
                 col_names)

def log2_buckets(config, delim, col, table, num_buckets, count_zeros=True, xattrs=False):
    lst = run_query(config, delim, [PINODE, col], [table], xattrs=xattrs)

    # sort results by directory
    by_dir = {}
    for pinode, val in lst:
        # length of nonexistant string is empty, not 0
        if len(val) == 0:
            continue

        if pinode in by_dir:
            by_dir[pinode] += [int(val)]
        else:
            by_dir[pinode] = [int(val)]

    # bucket each directory's values by int(log2(col))
    dir_msb = []
    for pinode, values in by_dir.items():
        zero_count = 0
        too_big = 0
        buckets = [0] * num_buckets

        for val in values:
            if val == 0:
                zero_count += 1
                continue

            msb = int(np.floor(np.log2(int(val))))

            # num_buckets -> [0, num_buckets), so keep track of
            # msb > num_buckets AND msb == num_buckets
            if msb >= num_buckets:
                too_big += 1
                continue

            buckets[msb] += 1

        # generate column
        row = [pinode]
        if count_zeros is True:
            row += [zero_count]
        row += buckets
        row += [too_big]

        dir_msb += [row]

    # generate column names
    cols = [PINODE]
    if count_zeros is True:
        cols += ['zero_count']
    cols += ['msb_{0}'.format(i) for i in range(num_buckets)]
    cols += ['msb_ge_{0}'.format(num_buckets)]

    return pd.DataFrame(dir_msb, columns=cols)

def time_buckets(config, delim, col_name, reftime):
    times = run_query(config, delim, [PINODE, col_name], [VRPENTRIES])

    time_units = [
        ['second',      1],
        ['minute',      60],
        ['hour',        3600],
        ['day',         86400],
        ['week',        604800],
        ['four_weeks',  2419200],
        ['year',        31536000],
        ['years',       0],
    ]

    pinodes = {} # each pinode has its own set of buckets
    for pinode, val in times:
        delta_time = reftime - int(val)

        if pinode not in pinodes:
            pinodes[pinode] = {unit : 0 for unit, _ in time_units}

        for unit, seconds in time_units[:-1]:
            if delta_time < seconds:
                pinodes[pinode][unit] += 1
                break
        else:
            pinodes[pinode][time_units[-1][0]] += 1

    # transform dictionary of buckets to list of lists
    lst = []
    for pinode, buckets in pinodes.items():
        lst.append([pinode] + [buckets[unit] for unit, _ in time_units]) # guarantee order

    # generate colum names
    time_keys = [name for name, _ in time_units]
    return pd.DataFrame(lst, columns=[PINODE] + time_keys)

# compute stats of numeric columns
# uid, gid, size, ctime, atime, mtime, crtime
def process_numeric_columns(conn, config, delim, num_size_buckets, reftime):
    # mapping of snapshot columns name to numeric GUFI columns in VRPENTRIES
    UID_COLS = {
        'min':        'dminuid',
        'max':        'dmaxuid',
        'num_unique': 'COUNT(DISTINCT uid)',
    }

    GID_COLS = {
        'min':        'dmingid',
        'max':        'dmaxgid',
        'num_unique': 'COUNT(DISTINCT gid)',
    }

    SIZE_COLS = {
        'min':    'dminsize',
        'max':    'dmaxsize',
        'sum':    'dtotsize',
        'mean':   'AVG(size)',
        'median': 'median(size)',
        'stdev':  'stdevp(size)',
    }

    TIME_COLS = lambda name: {
        'min':    'dmin{0}'.format(name),
        'max':    'dmax{0}'.format(name),
        'mean':   'AVG({0})'.format(name),
        'median': 'median({0})'.format(name),
        'stdev':  'stdevp({0})'.format(name),
    }

    CTIME_COLS  = TIME_COLS('ctime')
    ATIME_COLS  = TIME_COLS('atime')
    MTIME_COLS  = TIME_COLS('mtime')
    CRTIME_COLS = TIME_COLS('crtime')

    COLS = {
        'uid':    UID_COLS,
        'gid':    GID_COLS,
        'size':   SIZE_COLS,
        'ctime':  CTIME_COLS,
        'atime':  ATIME_COLS,
        'mtime':  MTIME_COLS,
        'crtime': CRTIME_COLS,
    }

    cols = {}
    for col_name, vals in COLS.items():
        # collect common stats
        aggreg_df = to_df(run_query(config, delim, [PINODE] + list(vals.values()), [VRPENTRIES]),
                          [PINODE] + list(vals.keys()))
        mode_df = mode(config, delim, col_name)

        # merge results
        df = pd.merge(aggreg_df, mode_df, on=PINODE)

        # append column specific stats
        if col_name == 'size':
            buckets_df = log2_buckets(config, delim, col_name, VRPENTRIES, num_size_buckets, True)
            df = pd.merge(df, buckets_df, on=PINODE)
        elif col_name in ['ctime', 'atime', 'mtime', 'crtime']:
            buckets_df = time_buckets(config, delim, col_name, reftime)
            df = pd.merge(df, buckets_df, on=PINODE)

        df.to_sql(col_name, conn, index=False, if_exists='fail')

        cols[col_name] = df.columns[1:] # drop pinode column name

    return cols

# process string oclumns
# name, linkname
def process_string_columns(conn, config, delim,
                           log_max_name_len):
    STR_COLS = [
        'name',
        'linkname',
    ]

    cols = {}

    for s in STR_COLS:
        select_str = [
            'MIN(LENGTH({0}))'.format(s),
            'MAX(LENGTH({0}))'.format(s),
            'AVG(LENGTH({0}))'.format(s),
            'median(LENGTH({0}))'.format(s),
            'stdevp(LENGTH({0}))'.format(s),
        ]

        col_str = [
            'min',
            'max',
            'mean',
            'median',
            'stdev',
        ]

        aggreg_df = to_df(run_query(config, delim, [PINODE] + select_str, [VRPENTRIES]),
                          [PINODE] + col_str)

        where = None
        if s == 'linkname:':
            where = ['type == \'l\'']

        mode_df = to_df(run_query(config, delim,
                                  [PINODE, 'LENGTH({0}) AS mode'.format(s), 'COUNT(mode) AS count'],
                                  [VRPENTRIES],
                                  where,
                                  ['mode'],
                                  ['count DESC'],
                                  1),
                        [PINODE, 'mode', 'mode_count'])

        df = pd.merge(aggreg_df, mode_df, on=PINODE)
        buckets_df = log2_buckets(config, delim, 'LENGTH({0})'.format(s), VRPENTRIES, log_max_name_len, False, False)
        df = pd.merge(df, buckets_df, on=PINODE)

        df.to_sql(s, conn, index=False, if_exists='fail')

        cols[s] = df.columns[1:]

    return cols

def process_xattr_columns(conn, config, delim,
                          log_max_name_len):
    XATTR_COLS = [
        'xattr_name',
        'xattr_value',
    ]

    cols = {}

    for xattr in XATTR_COLS:
        select_str = [
            'MIN(LENGTH({0}))'.format(xattr),
            'MAX(LENGTH({0}))'.format(xattr),
            'AVG(LENGTH({0}))'.format(xattr),
            'median(LENGTH({0}))'.format(xattr),
            'stdevp(LENGTH({0}))'.format(xattr)
        ]

        col_str = [
            'min',
            'max',
            'mean',
            'median',
            'stdev',
        ]

        aggreg_df = to_df(run_query(config, delim, [PINODE] + select_str, [VRXPENTRIES], xattrs=True),
                          [PINODE] + col_str)

        mode_df = to_df(run_query(config, delim,
                                  [PINODE, 'LENGTH({0}) AS mode'.format(xattr), 'COUNT(mode) AS count'],
                                  [VRXPENTRIES],
                                  None,
                                  ['mode'],
                                  ['count DESC'],
                                  1,
                                  True),
                        [PINODE, 'mode', 'mode_count'])

        df = pd.merge(aggreg_df, mode_df, on=PINODE)
        buckets_df = log2_buckets(config, delim, 'LENGTH({0})'.format(xattr), VRXPENTRIES, log_max_name_len, False, True)
        df = pd.merge(df, buckets_df, on=PINODE)

        df.to_sql(xattr, conn, index=False, if_exists='fail')

        cols[xattr] = df.columns[1:]

    return cols

# find the top 4 most popular extensions
# returns single set of results
def mode_extensions(conn, config, delim):
    TABLE_NAME = 'extension'
    COLS = [
        'extension_1', 'ext_count_1',
        'extension_2', 'ext_count_2',
        'extension_3', 'ext_count_3',
        'extension_4', 'ext_count_4',
    ]

    df = to_df(run_query(config, delim,
                         [PINODE,
                          # filenames without extensions are treated as NULL
                          '''CASE WHEN name NOT LIKE '%.%' THEN
                                 NULL
                             ELSE
                                 REPLACE(name, RTRIM(name, REPLACE(name, '.', '')), '')
                             END AS extension''',
                          'COUNT(*) AS count'],
                         [VRPENTRIES],
                         None,
                         [PINODE, 'extension'],
                         ['count DESC'],
                         4),
               [PINODE, 'extension', 'count'])

    # transpose top 4 results (a single column) of a directory into a single row
    lst = []
    pinode_lst = df[PINODE].unique()
    for pinode in pinode_lst:
        row = [pinode]
        same_dir = df[df[PINODE] == pinode]
        extensions = np.array(same_dir['extension'])
        counts = np.array(same_dir['count'])
        for i, _ in enumerate(extensions):
            row.append(extensions[i])
            row.append(counts[i])
        lst.append(row)

    pd.DataFrame([row + [np.nan] * (4 - len(row)) for row in lst],
                      columns=[PINODE] + COLS).to_sql(TABLE_NAME, conn, index=False, if_exists='fail')

    return {TABLE_NAME: COLS}

# find the top 4 most popular permission bits per directory
# returns single set of results
def permission_buckets(conn, config, delim):
    TABLE_NAME = 'permission'
    COLS = [
        'permission_1', 'perm_count_1',
        'permission_2', 'perm_count_2',
        'permission_3', 'perm_count_3',
        'permission_4', 'perm_count_4'
    ]

    df = to_df(run_query(config, delim,
                         [PINODE, 'mode'],
                         [VRPENTRIES]),
               [PINODE, 'mode'])

    pinodes = df[PINODE].unique()
    row_lst = []

    for pinode in pinodes:
        buckets = {i: 0 for i in range(0o1000)} # 512 for 9 permission bits

        # get all results for this directory
        curr_dir = df[df[PINODE] == pinode]
        modes = np.array(curr_dir['mode'])

        for mode in modes:
            perm = int(mode) & 0o777
            buckets[perm] += 1

        top_four = sorted(buckets.items(), key=lambda item: (item[1], item[0]), reverse=True)[:4]

        row_res = [pinode]
        for perm, count in top_four:
            if count != 0:
                row_res += [perm, count]
            else:
                row_res += [None, None]
        row_lst.append(row_res)

    pd.DataFrame(row_lst, columns=[PINODE] + COLS).to_sql(TABLE_NAME, conn, index=False, if_exists='fail')

    return {TABLE_NAME: COLS}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GUFI Longitudinal Snapshot Generator')
    parser.add_argument('outname',
                        help='output db file name')
    parser.add_argument('--reftime',      metavar='seconds', type=int,           default=int(time.time()),
                        help='reference point for age (since UNIX epoch)')
    parser.add_argument('--delim',        metavar='c',       type=get_char,      default='|',
                        help='delimiter of gufi_query output')
    parser.add_argument('--max_size',     metavar='pos_int', type=get_positive,  default=1 << 50,
                        help='the maximum expected size')
    parser.add_argument('--max_name_len', metavar='pos_int', type=get_positive,  default=1 << 8,
                        help='the maximum expected length of a name/linkname')
    parser.add_argument('--notes',        metavar='text',    type=str,           default=None,
                        help='freeform text of any extra information to add to the snapshot')

    args = parser.parse_args()
    config = gufi_config.Server(gufi_config.PATH)

    timestamp = int(time.time())

    with sqlite3.connect(args.outname) as conn:
        # create the metadata table
        # one row of data
        conn.execute('''
            CREATE TABLE IF NOT EXISTS {} (
                timestamp INT,
                src TEXT,
                notes TEXT
            );
        '''.format(METADATA_TABLE_NAME))

        conn.execute('''
            INSERT INTO {} (timestamp, src, notes)
            VALUES (?, ?, ?);
        '''.format(METADATA_TABLE_NAME),
                     (timestamp, config.indexroot(), args.notes))

        conn.commit()

        # run queries and get table schemas back
        schemas = {}
        schemas.update(process_numeric_columns(conn, config, args.delim, int(np.log2(args.max_size)), args.reftime))
        schemas.update(process_string_columns (conn, config, args.delim, int(np.log2(args.max_name_len))))
        schemas.update(process_xattr_columns  (conn, config, args.delim, int(np.log2(args.max_name_len))))
        schemas.update(mode_extensions        (conn, config, args.delim))
        schemas.update(permission_buckets     (conn, config, args.delim))

        # summary is used separately, so it is not added to the schemas dictionary
        summary_name, summary_cols = get_summary_data(conn, config, args.delim)

        snapshot_cols = ', '.join([', '.join('{0}.{1} AS {1}'.format(summary_name, summary_col)
                                             for summary_col in summary_cols)] +
                                  [', '.join('{0}.{1} AS {0}_{1}'.format(name, col) for col in cols)
                                   for name, cols in schemas.items()])
        snapshot_tables = 'summary ' + ' '.join('LEFT JOIN {0} ON summary.inode == {0}.pinode'.format(table_name)
                                                for table_name in schemas)

        view_of_everything = '''
            CREATE VIEW {0} AS
            SELECT {1}
            FROM {2};
        '''.format(SNAPSHOT_VIEW, snapshot_cols, snapshot_tables)

        conn.execute(view_of_everything)
        conn.commit()
