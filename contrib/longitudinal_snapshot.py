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
import math        # using math.log(x, 2) instead of math.log2(x) in case Python version is lower than 3.3
import os          # using os.devnull instead of subprocess.DEVNULL in case Python version is lower than 3.3
import sqlite3
import subprocess
import time
import sys

from gufi_common import SQLITE3_NULL, SQLITE3_INT64, SQLITE3_DOUBLE, SQLITE3_TEXT
from gufi_common import build_query, get_positive, print_query
from gufi_common import ENTRIES, VRXPENTRIES, SUMMARY, VRXSUMMARY, TREESUMMARY
from gufi_common import LEVEL, INODE, PINODE, PPINODE, UID, GID

METADATA = 'metadata' # user data
SNAPSHOT = 'snapshot' # SUMMARY left joined with ENTRIES and optional TREESUMMARY data

GROUP_BY = {
    'graph'     : None,
    'level'     : [LEVEL],
    'parent'    : [PINODE],
    PINODE      : [PINODE],
    'directory' : [INODE],
    INODE       : [INODE],
}

def parse_args(argv, now):
    parser = argparse.ArgumentParser(description='GUFI Longitudinal Snapshot Generator')

    parser.add_argument('--verbose', '-V', action='store_true',
                        help='Show the gufi_query being executed')

    # positional
    parser.add_argument('grouping', metavar='col',            choices=GROUP_BY.keys(),
                        help='column to group stats on')
    parser.add_argument('index',
                        help='index to snapshot')
    parser.add_argument('outname',
                        help='output db file name')

    # optional
    parser.add_argument('--reftime',       metavar='seconds', type=int,           default=now,
                        help='reference point for age (since UNIX epoch)')
    parser.add_argument('--max_size',      metavar='pos_int', type=get_positive,  default=1 << 50,
                        help='the maximum expected size')
    parser.add_argument('--max_name_len',  metavar='pos_int', type=get_positive,  default=1 << 8,
                        help='the maximum expected length of a name/linkname')
    parser.add_argument('--notes',         metavar='text',    type=str,           default=None,
                        help='freeform text of any extra information to add to the snapshot')
    parser.add_argument('--replace',       action='store_true',
                        help='replace existing tables')

    #  gufi_query args
    parser.add_argument('--gufi_query',    metavar='path',    type=str,           default='gufi_query',
                        help='path to gufi_query executable')
    parser.add_argument('--threads', '-n', metavar='count',   type=get_positive,  default=1,
                        help='thread count')

    return parser.parse_args(argv[1:])

def create_table(name, cols, temp):
    return 'CREATE {0} {1} ({2})'.format(
        'TEMP TABLE' if temp else 'TABLE',
        name,
        ', '.join(cols)
    )

class Stat: # pylint: disable=too-few-public-methods
    def __init__(self, col_name, col_type=SQLITE3_INT64, sql=None):
        self.col_name = col_name
        self.col_type = col_type
        self.sql      = sql if sql is not None else col_name

def gen_minmax_cols(col):
    return [
        Stat('min',        sql='MIN({0})'.format(col)),
        Stat('max',        sql='MAX({0})'.format(col)),
    ]

def gen_avg_cols(col):
    return [
        Stat('mean',       SQLITE3_DOUBLE, sql='AVG({0})'.format(col)),
        Stat('median',     SQLITE3_DOUBLE, sql='median({0})'.format(col)),
        Stat('mode',       SQLITE3_TEXT,   sql='mode_count({0})'.format(col)),
    ]

def gen_stdev_col(col):
    return [
        Stat('stdev',      SQLITE3_DOUBLE, sql='stdevp({0})'.format(col)),
    ]

def gen_stat_cols(col):
    return gen_minmax_cols(col) + gen_avg_cols(col) + gen_stdev_col(col)

def gen_log2_hist_col(col, buckets):
    return [
        Stat('hist',       SQLITE3_TEXT, 'log2_hist({0}, {1})'.format(col, buckets)),
    ]

# used to generate timestamp columns
def gen_time_cols(col, reftime):
    # min/max already captured by SUMMARY
    return gen_avg_cols(col) + gen_stdev_col(col) + [
        Stat('age_hist',   SQLITE3_TEXT, 'time_hist({0}, {1})'.format(col, reftime)),
        Stat('hour_hist',  SQLITE3_TEXT, 'category_hist(CAST({0} AS TEXT), 1)'.format('strftime(\'%H\', {0})'.format(col))),
    ]

def gen_ftime_cols():
    # Satyanarayanan, Mahadev. "A study of file sizes and functional lifetimes."
    # ACM SIGOPS Operating Systems Review 15.5 (1981): 96-108.
    ftime = 'atime - mtime'
    return gen_stat_cols(ftime)

# used to generate columns for name, linkname, xattr_name, and xattr_value
def gen_str_cols(col, buckets):
    return gen_stat_cols(col) + [
        Stat('hist',       SQLITE3_TEXT, 'log2_hist({0}, {1})'.format(col, buckets)),
    ]

def agg(col):
    TOT = 'tot'
    MIN = 'min'
    MAX = 'max'

    if col[:3] == MIN:
        return 'MIN({0})'.format(col)
    if col[:3] == MAX:
        return 'MAX({0})'.format(col)
    if col[:3] == TOT:
        return 'TOTAL({0})'.format(col)

    return col

def treesummary(group_by):
    COLS = [
        Stat(LEVEL,         sql='level()'),
        Stat(INODE,         SQLITE3_TEXT),
        Stat(PINODE,        SQLITE3_TEXT),
        Stat('totsubdirs'),
        Stat('maxsubdirfiles'),
        Stat('maxsubdirlinks'),
        Stat('maxsubdirsize'),
        Stat('totfiles'),
        Stat('totlinks'),
        Stat('minuid'),     Stat('maxuid'),
        Stat('mingid'),     Stat('maxgid'),
        Stat('minsize'),    Stat('maxsize'),
        Stat('totzero'),
        Stat('totltk'),
        Stat('totmtk'),
        Stat('totltm'),
        Stat('totmtm'),
        Stat('totmtg'),
        Stat('totmtt'),
        Stat('totsize'),
        Stat('minctime'),   Stat('maxctime'),
        Stat('minmtime'),   Stat('maxmtime'),
        Stat('minatime'),   Stat('maxatime'),
        Stat('minblocks'),  Stat('maxblocks'),
        Stat('totxattr'),
        Stat('mincrtime'),  Stat('maxcrtime'),
        Stat('minossint1'), Stat('maxossint1'), Stat('totossint1'),
        Stat('minossint2'), Stat('maxossint2'), Stat('totossint2'),
        Stat('minossint3'), Stat('maxossint3'), Stat('totossint3'),
        Stat('minossint4'), Stat('maxossint4'), Stat('totossint4'),
        # Stat('rectype'),
        # Stat(UID),
        # Stat(GID),
    ]

    IK = []
    T  = []
    JG = []

    # rename columns for SNAPSHOT
    V = [
        '{0} AS ts_{0}'.format(stat.col_name)
        for stat in COLS
    ]

    for stat in COLS:
        IK += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        T  += [stat.sql]
        JG += [agg(stat.col_name)]

    INTERMEDIATE = 'intermediate_treesummary'

    J_name = 'J_treesummary'

    return (
        # -I
        create_table(INTERMEDIATE, IK, False),

        # -T
        'INSERT INTO {0} {1}; SELECT 1'.format(INTERMEDIATE,
                                               build_query(T, [TREESUMMARY])),

        # -K
        '{0}; {1}'.format(create_table(J_name,      IK, True),
                          create_table(TREESUMMARY, IK, False)),

        # -J
        'INSERT INTO {0} {1}'.format(J_name,
                                     build_query(JG, [INTERMEDIATE],
                                                 None, group_by)),

        # -G
        'INSERT INTO {0} {1}; DROP TABLE {2}'.format(TREESUMMARY,
                                                     build_query(JG, [J_name],
                                                                 None, group_by),
                                                     J_name),

        # rename columns for SNAPSHOT
        build_query(V, [TREESUMMARY])
    )

def summary(group_by,                            # pylint: disable=too-many-locals
            reftime,
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    CONST_COLS = [
        Stat(LEVEL, sql='level()'),
        Stat(INODE),
        Stat(PINODE),
    ]

    DIR_COLS = [
        [Stat('name_len',   sql='LENGTH(basename(name))'),
                            lambda col : gen_str_cols(col, log2_name_len_bucket_count)],
        [Stat('mode'),      lambda col : [Stat('hist', SQLITE3_TEXT, 'mode_hist({0})'.format(col))]],
        [Stat('nlink'),     gen_stat_cols],
        [Stat(UID),         gen_minmax_cols],
        [Stat(GID),         gen_minmax_cols],
        [Stat('size'),      lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        [Stat('blksize'),   lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        [Stat('blocks'),    lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        [Stat('atime'),     lambda col : gen_minmax_cols(col) + gen_time_cols(col, reftime)],
        [Stat('mtime'),     lambda col : gen_minmax_cols(col) + gen_time_cols(col, reftime)],
        [Stat('ctime'),     lambda col : gen_minmax_cols(col) + gen_time_cols(col, reftime)],
    ]

    SUM_COLS = [
        Stat('totdirs',     sql='1'),
        Stat('totsubdirs',  sql='subdirs(srollsubdirs, sroll)'),
        Stat('minfiles',    sql='totfiles'),
        Stat('maxfiles',    sql='totfiles'),
        Stat('totfiles',    sql='totfiles'),
        Stat('minlinks',    sql='totlinks'),
        Stat('maxlinks',    sql='totlinks'),
        Stat('totlinks',    sql='totlinks'),
        Stat('minuid'),     Stat('maxuid'),
        Stat('mingid'),     Stat('maxgid'),
        Stat('minsize'),    Stat('maxsize'),    Stat('totsize'),
        Stat('totzero'),
        Stat('totltk'),
        Stat('totmtk'),
        Stat('totltm'),
        Stat('totmtm'),
        Stat('totmtg'),
        Stat('totmtt'),
        Stat('minossint1'), Stat('maxossint1'), Stat('totossint1'),
        Stat('minossint2'), Stat('maxossint2'), Stat('totossint2'),
        Stat('minossint3'), Stat('maxossint3'), Stat('totossint3'),
        Stat('minossint4'), Stat('maxossint4'), Stat('totossint4'),
    ]

    IJ = []
    S  = []
    K  = []
    J  = []
    G  = []

    for stat in CONST_COLS:
        IJ += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        S  += [stat.sql]
        J  += [stat.col_name]
        K  += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        G  += [stat.col_name]

    # uncomment this to be able to group by UID/GID
    # K  += ['{0} {1}'.format(UID, SQLITE3_INT64)]
    # G  += [UID]
    # K  += ['{0} {1}'.format(GID, SQLITE3_INT64)]
    # G  += [GID]

    for stat, g in DIR_COLS:
        IJ += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        S  += [stat.sql]
        J  += [stat.col_name]

        for g_stat in g(stat.col_name):
            K += ['dir_{0}_{1} {2}'.format(stat.col_name, g_stat.col_name, g_stat.col_type)]
            G += [g_stat.sql]

    for stat in gen_ftime_cols():
        K += ['dir_ftime_{0} {1}'.format(stat.col_name, stat.col_type)]
        G += [stat.sql]

    for stat in SUM_COLS:
        IJ += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        S  += [stat.sql]
        K  += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        J  += [stat.col_name]
        G  += [agg(stat.col_name)]

    INTERMEDIATE = 'intermediate_summary'

    J_name = 'J_summary'

    return (
        # -I
        create_table(INTERMEDIATE, IJ, False),

        # -S
        'INSERT INTO {0} {1}; SELECT 1'.format(INTERMEDIATE,
                                               build_query(S, [VRXSUMMARY],
                                                           None, [INODE])),

        # -K
        '{0}; {1}'.format(create_table(J_name,  IJ, True),
                          create_table(SUMMARY, K, False)),

        # -J
        # don't GROUP BY here - will lose directory stats
        'INSERT INTO {0} {1}'.format(J_name,
                                     build_query(J, [INTERMEDIATE])),

        # -G
        'INSERT INTO {0} {1}; DROP TABLE {2}'.format(SUMMARY,
                                                     build_query(G, [J_name],
                                                                 None, group_by),
                                                     J_name),
    )

def entries(group_by,                            # pylint: disable=too-many-locals
            reftime,
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    E_COLS = [
        Stat(LEVEL,
             sql='level()'),
        Stat(INODE,      SQLITE3_TEXT, PINODE),  # directory inode,  not file inode
        Stat(PINODE,     SQLITE3_TEXT, PPINODE), # directory pinode, not file pinode
        Stat('name_len', sql='LENGTH(basename(name))'),
        # Stat('type',     SQLITE3_TEXT),
        Stat('mode'),
        Stat('nlink'),
        Stat(UID),
        Stat(GID),
        Stat('size',
             sql='''
                 CASE WHEN type == \'f\' THEN
                     size
                 ELSE
                     {0}
                 END
                 '''.format(SQLITE3_NULL)),
        Stat('blksize'),
        Stat('blocks'),
        Stat('atime'),
        Stat('mtime'),
        Stat('ctime'),
        Stat('linkname_len',
             sql='''
                 CASE WHEN type == \'l\' THEN
                     LENGTH(linkname)
                 ELSE
                     {0}
                 END
                 '''.format(SQLITE3_NULL)),
        # Stat('xattr_name_len',  sql='LENGTH(xattr_name)'),
        # Stat('xattr_value_len', sql='LENGTH(xattr_value)'),
        Stat('crtime'),
        Stat('ossint1'),
        Stat('ossint2'),
        Stat('ossint3'),
        Stat('ossint4'),
        Stat('osstext1_len', sql='LENGTH(osstext1)'),
        Stat('osstext2_len', sql='LENGTH(osstext2)'),

        # filenames without extensions pass in NULL
        Stat('extension', SQLITE3_TEXT,
             # specify argument name for style consistency
             sql='''
                 CASE WHEN name NOT LIKE '%.%' THEN
                     {0}
                 ELSE
                     REPLACE(name, RTRIM(name, REPLACE(name, '.', '')), '')
                 END
                '''.format(SQLITE3_NULL)), # off by 1 space for style consistency
    ]

    UID_COLS = [
        # min/max already captured by SUMMARY
        Stat('hist',        SQLITE3_TEXT, 'category_hist(CAST({0} AS TEXT), 1)'.format(UID)),
        Stat('num_unique',  sql='COUNT(DISTINCT {0})'.format(UID))
    ]

    GID_COLS = [
        # min/max already captured by SUMMARY
        Stat('hist',        SQLITE3_TEXT, 'category_hist(CAST({0} AS TEXT), 1)'.format(GID)),
        Stat('num_unique',  sql='COUNT(DISTINCT {0})'.format(GID))
    ]

    # min, max, and sum already captured by SUMMARY
    SIZE_COLS = gen_avg_cols('size') + gen_stdev_col('size') + gen_log2_hist_col('size', log2_size_bucket_count)

    PERM_COLS = [
        Stat('hist', SQLITE3_TEXT, 'mode_hist(mode)'),
    ]

    ATIME_COLS       = gen_time_cols('atime',                    reftime)
    MTIME_COLS       = gen_time_cols('mtime',                    reftime)
    CTIME_COLS       = gen_time_cols('ctime',                    reftime)
    CRTIME_COLS      = gen_time_cols('crtime',                   reftime)

    FTIME_COLS       = gen_ftime_cols()

    NAME_COLS        = gen_str_cols('name_len',                  log2_name_len_bucket_count)
    LINKNAME_COLS    = gen_str_cols('linkname_len',              log2_name_len_bucket_count)
    # TODO: need to pull xattrs separately since there might be multiple pairs per entry # pylint: disable=fixme
    # XATTR_NAME_COLS  = gen_str_cols('xattr_name_len',            log2_name_len_bucket_count)
    # XATTR_VALUE_COLS = gen_str_cols('xattr_value_len',           log2_name_len_bucket_count)

    EXT_COLS = [
        Stat('hist', SQLITE3_TEXT, 'category_hist(extension, 1)'),
    ]

    EXT_LEN_COLS     = gen_str_cols('LENGTH(extension)',         log2_name_len_bucket_count)

    G_COLS = [
        ['uid',             UID_COLS],
        ['gid',             GID_COLS],
        ['size',            SIZE_COLS],
        ['permissions',     PERM_COLS],
        ['ctime',           CTIME_COLS],
        ['atime',           ATIME_COLS],
        ['mtime',           MTIME_COLS],
        ['crtime',          CRTIME_COLS],
        ['ftime',           FTIME_COLS],
        ['name_len',        NAME_COLS],
        ['linkname_len',    LINKNAME_COLS],
        # ['xattr_name_len',  XATTR_NAME_COLS],
        # ['xattr_value_len', XATTR_VALUE_COLS],
        ['extensions',      EXT_COLS],
        ['extensions_len',  EXT_LEN_COLS],
    ]

    I = []

    E = []

    K = [
        '{0} {1}'.format(LEVEL,  SQLITE3_INT64),
        '{0} {1}'.format(INODE,  SQLITE3_TEXT),
        '{0} {1}'.format(PINODE, SQLITE3_TEXT),
        '{0} {1}'.format(UID,  SQLITE3_INT64),
        '{0} {1}'.format(GID,  SQLITE3_INT64),
    ]

    G = [
        LEVEL,
        INODE,
        PINODE,
        UID,
        GID,
    ]

    for stat in E_COLS:
        I += ['{0} {1}'.format(stat.col_name, stat.col_type)]
        E += [stat.sql]

    for col_name, derived in G_COLS:
        for d in derived:
            K += ['{0}_{1} {2}'.format(col_name, d.col_name, d.col_type)]
            G += [d.sql]

    INTERMEDIATE = 'intermediate_entries'

    J_name = 'J_entries'

    return (
        # -I
        create_table(INTERMEDIATE, I, False),

        # -E
        'INSERT INTO {0} {1}'.format(INTERMEDIATE,
                                     build_query(E, [VRXPENTRIES],
                                                 None, [INODE])),

        # -K
        '{0}; {1}'.format(create_table(J_name,  I, True),
                          create_table(ENTRIES, K, False)),

        # -J
        'INSERT INTO {0} {1}'.format(J_name,
                                     build_query(['*'], [INTERMEDIATE])),

        # -G
        'INSERT INTO {0} {1}; DROP TABLE {2}'.format(ENTRIES,
                                                     build_query(G, [J_name],
                                                                 None, group_by),
                                                     J_name),
    )

def snapshot(group_by, ts_alias):
    group_by = None if group_by is None else group_by[0]
    return '\n        '.join([
        'CREATE VIEW {0}',
        'AS',
        '  SELECT *',
        '  FROM   {2}',
        '         LEFT JOIN {3}',
        '                ON {2}.{1} == {3}.{1}' if group_by is not None else '',
        '         LEFT JOIN ({4}) AS ts',
        '                ON {2}.{1} == ts.ts_{1}'if group_by is not None else '',
    ]).format(SNAPSHOT, group_by, SUMMARY, ENTRIES, ts_alias)

def run(argv): # pylint: disable=too-many-locals
    timestamp = int(time.time())

    args = parse_args(argv, timestamp)

    group_by = GROUP_BY[args.grouping]
    log2_size_bucket_count = int(math.ceil(math.log(args.max_size, 2)))
    log2_name_len_bucket_count = int(math.ceil(math.log(args.max_name_len, 2)))

    # ############################################################################
    # get SQL strings for constructing command
    ts_I, T, ts_K, ts_J, ts_G, ts_V = treesummary(group_by)
    sum_I, S, sum_K, sum_J, sum_G = summary(group_by,
                                            args.reftime,
                                            log2_size_bucket_count,
                                            log2_name_len_bucket_count)
    ent_I, E, ent_K, ent_J, ent_G = entries(group_by,
                                            args.reftime,
                                            log2_size_bucket_count,
                                            log2_name_len_bucket_count)
    # ############################################################################

    # construct full command to run

    # ############################################################################
    I = '''
        {0};
        {1};
        {2};
        '''.format(ts_I, sum_I, ent_I)
    # ############################################################################

    # ############################################################################
    K = '''
        {0};
        {1};
        {2};
        '''.format(ts_K, sum_K, ent_K)

    if args.replace:
        K = '''
            DROP TABLE IF EXISTS {0};
            DROP TABLE IF EXISTS {1};
            DROP TABLE IF EXISTS {2};
            {3}
            '''.format(TREESUMMARY, SUMMARY, ENTRIES, K)
    ############################################################################

    # ############################################################################
    J = '''
        {0};
        {1};
        {2};
        '''.format(ts_J, sum_J, ent_J)
    # ############################################################################

    # ############################################################################
    G = '''
        {0};
        {1};
        {2};
        {3};
        '''.format(ts_G, sum_G, ent_G,
                   snapshot(group_by, ts_V))

    if args.replace:
        G = 'DROP VIEW IF EXISTS {0}; {1}'.format(SNAPSHOT, G)
    # ############################################################################

    cmd = [
        args.gufi_query,
        '-n', str(args.threads),
        '-x',
        '-O', args.outname,
        '-I', I,
        '-T', T,
        '-S', S,
        '-E', E,
        '-K', K,
        '-J', J,
        '-G', G,
        args.index,
    ]

    if args.verbose:
        print_query(cmd)

    rc = 0

    # nothing should be printed to stdout
    # however, due to how gufi_query is implemented, -T
    # will print some text that is not used, so drop it
    with open(os.devnull, 'w') as DEVNULL:                    # pylint: disable=unspecified-encoding
        query = subprocess.Popen(cmd, stdout=DEVNULL)         # pylint: disable=consider-using-with
        query.communicate()                                   # block until query finishes
        rc = query.returncode

    if rc:
        return rc

    with sqlite3.connect(args.outname) as conn:
        if args.replace:
            conn.execute('DROP TABLE IF EXISTS {0};'.format(
                METADATA))

        # create the metadata table (one row of data)
        conn.execute('''
            CREATE TABLE {0} (
                timestamp INT,
                src TEXT,
                notes TEXT
            );
        '''.format(METADATA))

        conn.execute('''
            INSERT INTO {0} (timestamp, src, notes)
            VALUES (?, ?, ?);
        '''.format(METADATA),
                     (timestamp, args.index, args.notes))

        conn.commit()

    return 0

if __name__ == '__main__':
    sys.exit(run(sys.argv))
