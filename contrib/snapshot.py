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



import subprocess
import sqlite3
import time
import argparse
import pandas as pd
import numpy as np

from gufi_common import build_query, VRSUMMARY, VRPENTRIES
import gufi_config

META_TABLE_NAME = 'snap_meta'
SNAPSHOT_VIEW = 'snapshot'
PINODE = 'pinode'

def run_command(command):
    output_bytes = subprocess.check_output(command, shell=True)
    output_string = output_bytes.decode('utf-8')
    return output_string

def output_to_array(output):
    data = output.split('\n')
    data_lst = [row.split('|') for row in data[:-1]]
    return np.array(data_lst)

# pylint: disable=too-many-arguments
def run_query(index, config, names, columns, tables, where=None, group_by=None, order_by=None, num_results=None):
    query = build_query(columns, tables, where, group_by, order_by, num_results)
    command = 'sudo {} {} -n {} -d "|" -E "{};"'.format(config.query(), index, config.threads(), query)
    output = run_command(command)
    output_arr = output_to_array(output)
    if len(output_arr) == 0:
        return pd.DataFrame(columns=names)
    return pd.DataFrame(output_arr, columns=names)

def permission_buckets(index, config):
    df = run_query(index,
                   config,
                   [PINODE, 'mode'],
                   [PINODE, 'mode'],
                   [VRPENTRIES])

    pinode_lst = df[PINODE].unique()
    row_lst = []

    for node in pinode_lst:
        df_2 = df[df[PINODE] == node]
        mode_lst = np.array(df_2['mode'])
        buckets = {i: 0 for i in range(512)}

        for m in mode_lst:
            permission = (int(m) & 0o777)
            buckets[permission] += 1

        top_four = dict(sorted(buckets.items(), key=lambda item: item[1], reverse=True)[:4])
        top_four_lst = [node]

        for permission, count in top_four.items():
            if count != 0:
                top_four_lst.extend([permission, count])
            else:
                top_four_lst.extend([None, None])
        row_lst.append(top_four_lst)

    return pd.DataFrame(row_lst, columns=[PINODE,
                                         'permission_1', 'perm_count_1',
                                         'permission_2', 'perm_count_2',
                                         'permission_3', 'perm_count_3',
                                         'permission_4', 'perm_count_4'])

def mode_extensions(index, config):
    df = run_query(index,
                   config,
                   [PINODE, 'extension', 'count'],
                   [PINODE,
                    "CASE WHEN name NOT LIKE \'%.%\' THEN NULL ELSE REPLACE(name, RTRIM(name, REPLACE(name, \'.\', \'\')), \'\') END AS extension",
                    'COUNT(*) AS count'],
                   [VRPENTRIES],
                   group_by=[PINODE, 'extension'],
                   order_by=['count DESC'],
                   num_results='4')
    return extensions_reshape(df)

def extensions_reshape(df):
    lst = []
    pinode_lst = df[PINODE].unique()
    for node in pinode_lst:
        row = [node]
        df_2 = df[df[PINODE] == node]
        extensions = np.array(df_2['extension'])
        counts = np.array(df_2['count'])
        for i, _ in enumerate(extensions):
            row.append(extensions[i])
            row.append(counts[i])
        lst.append(row)
    df = pd.DataFrame([row + [np.nan] * (4 - len(row)) for row in lst], columns=[PINODE,
                                                                                 'extension_1', 'ext_count_1',
                                                                                 'extension_2', 'ext_count_2',
                                                                                 'extension_3', 'ext_count_3',
                                                                                 'extension_4', 'ext_count_4'])
    return df

def column_names_from_db(cursor, table_name):
    cursor.execute("PRAGMA TABLE_INFO({})".format(table_name))
    return [column[1] for column in cursor.fetchall() if column[1] != PINODE]

def dfs_to_db(index, file_name, dfs, table_names): #add flexibility to add more metadata
    if len(dfs) != len(table_names):
        raise ValueError('Number of given table_names do not match number of dataframes')

    timestamp = int(time.time())

    with sqlite3.connect('{}.db'.format(file_name)) as conn:
        for i, _ in enumerate(table_names):
            dfs[i].to_sql(table_names[i], conn, index=False, if_exists='fail')

        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS {} (
                timestamp INT,
                src TEXT
            )
        '''.format(META_TABLE_NAME))

        conn.commit()

        cursor.execute('''
            INSERT INTO {} (timestamp, src)
            VALUES (?, ?)
        '''.format(META_TABLE_NAME), (timestamp, index))

        conn.commit()

        select_clauses = []
        join_clauses = []
        for table_name in table_names:
            if table_name != 'summary':
                columns = column_names_from_db(cursor, table_name)
                select_clauses.append(", ".join("{}.".format(table_name) + "{}".format(column) for column in columns))
                join_clauses.append("LEFT JOIN {0} ON summary.inode = {0}.pinode".format(table_name))

        final_query = '''
            CREATE VIEW IF NOT EXISTS {} AS
            SELECT summary.*,  {}
            FROM summary {}
        '''.format(SNAPSHOT_VIEW, ", ".join(select_clauses), " ".join(join_clauses))

        cursor.execute(final_query)
        conn.commit()

def command_to_lst(data, index, config):
    command = "sudo {} {} -n {} -d '|' -E 'SELECT {}, {} FROM {};'".format(config.query(), index, config.threads(), PINODE, data, VRPENTRIES)
    output = run_command(command)
    return  output_to_array(output)

def log_2_buckets(data, name, num_buckets, index, config):
    lst = command_to_lst(data, index, config)
    rows = {}
    for node, val in lst:
        if node in rows:
            rows[node] += [int(val)]
        else:
            rows[node] = [int(val)]

    lst_2 = []
    for node, count  in rows.items():
        zero_count = 0
        buckets = [0 for _ in range(num_buckets)]
        for val in count:
            if val != 0:
                key = int(np.floor(np.log2(val)))
                buckets[key] += 1
            else:
                zero_count += 1
        lst = [node] + [zero_count] + buckets
        lst_2 += [lst]

    return pd.DataFrame(lst_2, columns=[PINODE] + ['{}_zero_count'.format(name)] + ['{}_msb_'.format(name) + str(i) for i in range(num_buckets)])

def mode(index, config, col):
    select_cols = [PINODE, col, 'COUNT({}) AS count'.format(col)]
    col_names = [PINODE, '{}_mode'.format(col), '{}_mode_count'.format(col)]

    return run_query(index,
                     config,
                     col_names,
                     select_cols,
                     [VRPENTRIES],
                     group_by=[col],
                     order_by=['count DESC'],
                     num_results='1')

def time_buckets(data, index, config):
    lst = command_to_lst(data, index, config)
    time_keys = ['second', 'minute', 'hour', 'day', 'week', '4_weeks', 'year', 'years']
    buckets = {}
    for node, val in lst:
        delta_time = args.user_time - int(val)
        if node not in buckets:
            buckets[node] = {key: 0 for key in time_keys} #line 188
        if delta_time < 1:
            buckets[node]['second'] += 1
        elif delta_time < 60:
            buckets[node]['minute'] += 1
        elif delta_time < 3600:
            buckets[node]['hour'] += 1
        elif delta_time < 86400:
            buckets[node]['day'] += 1
        elif delta_time < 604800:
            buckets[node]['week'] += 1
        elif delta_time < 2419200:
            buckets[node]['4_weeks'] += 1
        elif delta_time < 31536000:
            buckets[node]['year'] += 1
        else:
            buckets[node]['years'] += 1
    lst = []
    for key, value in buckets.items():
        lst.append([key] + list(value.values()))
    for i, _ in enumerate(time_keys):
        time_keys[i] = data + '_' + time_keys[i]
    df = pd.DataFrame(lst, columns=[PINODE] + time_keys)
    return df

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--index", default = "/tmp/index/build", help="path to index")
    parser.add_argument("file_name", help="file name for db file")
    parser.add_argument("--user_time", default=int(time.time()), help="time given by user for time buckets")

    args = parser.parse_args()
    configs = gufi_config.Server(gufi_config.PATH)

    UID_COLS = {
               'uid_min': 'dminuid',
               'uid_max': 'dmaxuid',
               'uid_num_unique': 'COUNT(DISTINCT uid)'
               }

    GID_COLS = {
               'gid_min': 'dmingid',
       	       'gid_max': 'dmaxgid',
       	       'gid_num_unique': 'COUNT(DISTINCT gid)'
               }

    SIZE_COLS = {
                'size_min': 'dminsize',
                'size_max': 'dmaxsize',
                'size_sum': 'dtotsize',
                'size_mean': 'AVG(size)',
                'size_median': 'median(size)',
                'size_stdev': 'stdevp(size)'
                }

    CTIME_COLS = {
                 'ctime_min': 'dminctime',
                 'ctime_max': 'dmaxctime',
                 'ctime_mean': 'AVG(ctime)',
                 'ctime_median': 'median(ctime)',
                 'ctime_stdev': 'stdevp(ctime)'
                 }

    ATIME_COLS = {
                 'atime_min': 'dminatime',
                 'atime_max': 'dmaxatime',
                 'atime_mean': 'AVG(atime)',
       	       	 'atime_median': 'median(atime)',
       	       	 'atime_stdev':	'stdevp(atime)'
               	 }

    MTIME_COLS = {
                 'mtime_min': 'dminmtime',
                 'mtime_max': 'dmaxmtime',
               	 'mtime_mean': 'AVG(mtime)',
       	       	 'mtime_median': 'median(mtime)',
       	       	 'mtime_stdev':	'stdevp(mtime)'
                  }

    CRTIME_COLS = {
                  'crtime_min': 'dmincrtime',
                  'crtime_max': 'dmaxcrtime',
               	  'crtime_mean': 'AVG(crtime)',
       	       	  'crtime_median': 'median(crtime)',
       	       	  'crtime_stdev': 'stdevp(ctime)'
                  }

    COLS = {
           'uid': UID_COLS,
           'gid': GID_COLS,
           'size': SIZE_COLS,
           'ctime': CTIME_COLS,
           'atime': ATIME_COLS,
           'mtime': MTIME_COLS,
           'crtime': CRTIME_COLS
           }

    NAMES = ['name', 'linkname']

    TIMES = ['ctime', 'atime', 'mtime', 'crtime']

    SUMMARY_COLS = [
                   'name', 'mode',
                   'uid', 'gid',
                   'size', 'blksize','blocks',
                   'atime', 'mtime', 'ctime',
                   'nlink', 'linkname',
                   'level()',
                   'NULL', #filesystem_type
                   'pinode', 'inode',
                   'totfiles', 'totlinks', 'subdirs(srollsubdirs, sroll)',
                   ]

    df_summary_cols = SUMMARY_COLS.copy()
    df_summary_cols[-7] = 'depth'
    df_summary_cols[-1] = 'totsubdirs'
    df_summary_cols[-6] = 'filesystem_type'

    summary_df = run_query(args.index,
                           configs,
                           df_summary_cols,
                           SUMMARY_COLS,
                           [VRSUMMARY])

    df_lst = []
    for col_name, vals  in COLS.items():
        aggreg_df = run_query(args.index,
                              configs,
                              list(vals.keys()) + [PINODE],
                              list(vals.values()) + [PINODE],
                              [VRPENTRIES])
        mode_df = mode(args.index, configs, col_name)
        merged_df = pd.merge(aggreg_df, mode_df, on=PINODE)
        if col_name == 'size':
            buckets_df = log_2_buckets(col_name, col_name, 50, args.index, configs)
            merged_df = pd.merge(merged_df, buckets_df, on=PINODE)
        elif col_name in TIMES:
            buckets_df = time_buckets(col_name, args.index, configs)
            merged_df = pd.merge(merged_df, buckets_df, on=PINODE)
        df_lst.append(merged_df)

    for n in NAMES:
        select_n = [PINODE, 'MIN(LENGTH({}))'.format(n), 'MAX(LENGTH({}))'.format(n),
                    'AVG(LENGTH({}))'.format(n), 'median(LENGTH({}))'.format(n), 'stdevp(LENGTH({}))'.format(n)]
        col_n = [PINODE, '{}_min'.format(n), '{}_max'.format(n), '{}_mean'.format(n), '{}_median'.format(n), '{}_stdev'.format(n)]
        aggreg_df = run_query(args.index,
                              configs,
                              col_n,
                              select_n,
                              [VRPENTRIES])
        if n == 'name':
            mode_df = run_query(args.index,
                                configs,
                                [PINODE, 'mode_name', 'mode_count_name'],
                                [PINODE, 'length(name) AS mode', 'COUNT(mode) AS count'],
                                [VRPENTRIES],
                                group_by=['mode'],
                                order_by=['count DESC'],
                                num_results='1')
        else:
            mode_df = run_query(args.index,
                                configs,
                                [PINODE, 'linkname_mode', 'linkname_mode_count'],
                                [PINODE, 'length(linkname) as mode', 'COUNT(mode) as count'],
                                [VRPENTRIES],
                                where=["type == \'l\'"],
                                group_by=['mode'],
                                order_by=['count DESC'],
                                num_results='1')
        merged_df = pd.merge(aggreg_df, mode_df, on=PINODE)
        buckets_df = log_2_buckets('LENGTH({})'.format(n), n, 10, args.index, configs)
        merged_df = pd.merge(merged_df, buckets_df, on=PINODE)
        df_lst.append(merged_df)

    extensions_df = mode_extensions(args.index, configs)
    df_lst.append(extensions_df)

    permissions_df = permission_buckets(args.index, configs)
    df_lst.append(permissions_df)

    df_lst.append(summary_df)

    df_names = list(COLS.keys()) + NAMES + ['extension', 'permission', 'summary']

    dfs_to_db(args.index, args.file_name, df_lst, df_names)
    #26 queries
