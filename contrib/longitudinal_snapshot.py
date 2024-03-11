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
import stat
import sqlite3
import subprocess
import time
import sys

from gufi_common import SQLITE3_NULL, SQLITE3_INT64, SQLITE3_DOUBLE, SQLITE3_TEXT, SQLITE3_BLOB
from gufi_common import build_query, get_non_negative, get_positive, print_query
from gufi_common import ENTRIES, VRXPENTRIES, SUMMARY, VRXSUMMARY, TREESUMMARY
from gufi_common import LEVEL, INODE, PINODE, PPINODE, UID, GID

FLATDB   = 'flatdb'   # ATTACH alias for flattened index data
METADATA = 'metadata' # user data
TSUM     = 'tsum'     # alias/prefix for SNAPSHOT
SUM      = 'sum'      # alias/prefix for SNAPSHOT
SNAPSHOT = 'snapshot' # SUMMARY left joined with ENTRIES and optional TREESUMMARY data

GROUP_BY = {
    'graph'     : None,
    'level'     : [LEVEL],
    'siblings'  : [PINODE],
    PINODE      : [PINODE],
    'directory' : [INODE],
    INODE       : [INODE],
}

def parse_args(argv, now):
    parser = argparse.ArgumentParser(description='GUFI Longitudinal Snapshot Generator')

    parser.add_argument('--verbose', '-V', action='store_true',
                        help='Show the gufi_query being executed')

    # positional
    parser.add_argument('view',                                 choices=sorted(GROUP_BY.keys()),
                        help='column to group stats on')
    parser.add_argument('index',                                type=str,
                        help='index to snapshot')
    parser.add_argument('flatdb',                               type=str,
                        help='flattend index data db filename')
    parser.add_argument('outdb',                                type=str,
                        help='output db filename')

    # optional
    parser.add_argument('--reftime',         metavar='seconds', type=int,             default=now,
                        help='reference point for age (since UNIX epoch)')
    parser.add_argument('--max_size',        metavar='pos_int', type=get_positive,    default=1 << 50,
                        help='the maximum expected size')
    parser.add_argument('--max_name_len',    metavar='pos_int', type=get_positive,    default=1 << 8,
                        help='the maximum expected length of a name/linkname')
    parser.add_argument('--notes',           metavar='text',    type=str,             default=None,
                        help='freeform text of any extra information to add to the snapshot')
    parser.add_argument('--replace-flatdb',  action='store_true',
                        help='replace data in index data db file')
    parser.add_argument('--replace-outdb',   action='store_true',
                        help='replace existing tables')

    #  gufi_query args
    parser.add_argument('--gufi_query',      metavar='path',    type=str,              default='gufi_query',
                        help='path to gufi_query executable')
    parser.add_argument('--threads', '-n',   metavar='count',   type=get_positive,     default=1,
                        help='thread count')
    parser.add_argument('--min-level', '-y', metavar='level',   type=get_non_negative, default=0,
                        help='level to start querying (min_level <= current_level)')
    parser.add_argument('--max-level', '-z', metavar='level',   type=get_non_negative, default=(1 << 64) - 1,
                        help='level to stop querying (current_level < max_level)')

    return parser.parse_args(argv[1:])

def run(cmd):
    rc = 0

    # nothing should be printed to stdout
    # however, due to how gufi_query is implemented, -T
    # will print some text that is not used, so drop it
    with open(os.devnull, 'w') as DEVNULL:                    # pylint: disable=unspecified-encoding
        query = subprocess.Popen(cmd, stdout=DEVNULL)         # pylint: disable=consider-using-with
        query.communicate()                                   # block until query finishes
        rc = query.returncode

    return rc

class Stat: # pylint: disable=too-few-public-methods,redefined-builtin
    def __init__(self, name, type=SQLITE3_INT64, sql=None):
        self.name = name
        self.type = type
        self.sql  = 'CAST({0} AS {1})'.format(sql if sql is not None else name, self.type)

# Satyanarayanan, Mahadev. "A study of file sizes and functional lifetimes."
# ACM SIGOPS Operating Systems Review 15.5 (1981): 96-108.
FTIME = Stat('ftime',    sql='atime - mtime')

T_COLS = [
    Stat(LEVEL,          sql='level()'),
    Stat(INODE,          SQLITE3_TEXT),
    Stat(PINODE,         SQLITE3_TEXT),
    Stat('totsubdirs'),
    Stat('maxsubdirfiles'),
    Stat('maxsubdirlinks'),
    Stat('maxsubdirsize'),
    Stat('totfiles'),
    Stat('totlinks'),
    Stat('minuid'),      Stat('maxuid'),
    Stat('mingid'),      Stat('maxgid'),
    Stat('minsize'),     Stat('maxsize'),
    Stat('totzero'),
    Stat('totltk'),
    Stat('totmtk'),
    Stat('totltm'),
    Stat('totmtm'),
    Stat('totmtg'),
    Stat('totmtt'),
    Stat('totsize'),
    Stat('minctime'),    Stat('maxctime'),
    Stat('minmtime'),    Stat('maxmtime'),
    Stat('minatime'),    Stat('maxatime'),
    Stat('minblocks'),   Stat('maxblocks'),
    Stat('totxattr'),
    # Stat('depth'),
    Stat('mincrtime'),   Stat('maxcrtime'),
    Stat('minossint1'),  Stat('maxossint1'), Stat('totossint1'),
    Stat('minossint2'),  Stat('maxossint2'), Stat('totossint2'),
    Stat('minossint3'),  Stat('maxossint3'), Stat('totossint3'),
    Stat('minossint4'),  Stat('maxossint4'), Stat('totossint4'),
    # Stat('rectype'),
    # Stat(UID),
    # Stat(GID),
]

S_COLS = [
    Stat(LEVEL,          sql='level()'),
    Stat('fs_type',      SQLITE3_BLOB,
                         sql=SQLITE3_NULL),
    Stat('name_len',     sql='LENGTH(name)'),
    Stat(INODE),
    Stat('mode'),
    Stat('nlink'),
    Stat(UID),
    Stat(GID),
    Stat('size'),
    Stat('blksize'),
    Stat('blocks'),
    Stat('atime'),
    Stat('mtime'),
    Stat('ctime'),
    FTIME,
    # Stat('linkname'),
    # Stat('xattr_names'),
    Stat('totfiles',     sql='totfiles'),
    Stat('totlinks',     sql='totlinks'),
    Stat('totsubdirs',   sql='subdirs(srollsubdirs, sroll)'),
    Stat('minuid'),      Stat('maxuid'),
    Stat('mingid'),      Stat('maxgid'),
    Stat('minsize'),     Stat('maxsize'),    Stat('totsize'),
    Stat('totzero'),
    Stat('totltk'),
    Stat('totmtk'),
    Stat('totltm'),
    Stat('totmtm'),
    Stat('totmtg'),
    Stat('totmtt'),
    Stat('minossint1'),  Stat('maxossint1'), Stat('totossint1'),
    Stat('minossint2'),  Stat('maxossint2'), Stat('totossint2'),
    Stat('minossint3'),  Stat('maxossint3'), Stat('totossint3'),
    Stat('minossint4'),  Stat('maxossint4'), Stat('totossint4'),
    # Stat('rectype'),
    Stat(PINODE),
    # Stat('isroot'),
    # Stat('rollupscore'),

    # TODO: add some VRXSUMMARY columns # pylint: disable=fixme
]

E_COLS = [
    Stat(LEVEL,          sql='level()'),
    Stat('name_len',     sql='LENGTH(basename(name))'),
    # filenames without extensions pass in NULL
    Stat('extension',    SQLITE3_TEXT,
                         sql='''
                             CASE WHEN name LIKE '%.%' THEN
                                 REPLACE(name, RTRIM(name, REPLACE(name, '.', '')), '')
                             ELSE
                                 {0}
                             END
                             '''.format(SQLITE3_NULL)),
    # extension length is calculated later
    Stat('type',         SQLITE3_TEXT),
    Stat(INODE,          SQLITE3_TEXT),
    Stat('mode'),
    Stat('nlink'),
    Stat(UID),
    Stat(GID),
    Stat('size'),
    Stat('blksize'),
    Stat('blocks'),
    Stat('atime'),
    Stat('mtime'),
    Stat('ctime'),
    FTIME,
    Stat('linkname_len', sql='''
                             CASE WHEN type == 'l' THEN
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

    Stat(PINODE,     SQLITE3_TEXT, PINODE),
    Stat(PPINODE,    SQLITE3_TEXT, PPINODE),

    # TODO: add some VRXPENTRIES columns # pylint: disable=fixme
]

def create_table(name, cols):
    return '''DROP TABLE IF EXISTS {0};
              CREATE TABLE {0}({1});'''.format(
                  name, ', ' .join(['{0} {1}'.format(col.name, col.type)
                                    for col in cols]))

# pull raw values from the index
# transform any non-numeric values to numeric data points
# amortizes cost of walking index multiple times
def flatten_index(args):
    query = False

    st = None
    # check if path exists
    try:
        st = os.stat(args.flatdb)
    except: # pylint: disable=bare-except
        query = True

    # if the path exists, make sure it is a file
    if st:
        if stat.S_ISREG(st.st_mode):
            query = args.replace_flatdb
        else:
            raise TypeError('"{0}" is not a regular file\n'.format(args.flatdb))

    # don't repull data if not necessary
    # not checking contents of file at path
    if not query:
        return 0

    # per thread db table names
    IT = 'intermediate_treesummary'
    IS = 'intermediate_summary'
    IE = 'intermediate_entries'

    I = '''{0}
           {1}
           {2}'''.format(create_table(IT, T_COLS),
                         create_table(IS, S_COLS),
                         create_table(IE, E_COLS))

    T = 'INSERT INTO {0} SELECT {1} FROM {2}; SELECT 1;'.format(
        IT,
        ', '.join([col.sql for col in T_COLS]),
        TREESUMMARY)

    S = 'INSERT INTO {0} SELECT {1} FROM {2} GROUP BY {3}; SELECT 1;'.format(
        IS,
        ', '.join([col.sql for col in S_COLS]),
        VRXSUMMARY, INODE)  # TODO: GROUP BY inode to not deal with xattrs for now # pylint: disable=fixme

    E = 'INSERT INTO {0} SELECT {1} FROM {2} GROUP BY {3}; SELECT 1;'.format(
        IE,
        ', '.join([col.sql for col in E_COLS]),
        VRXPENTRIES, INODE) # TODO: GROUP BY inode to not deal with xattrs for now # pylint: disable=fixme

    K = '''{0}
           {1}
           {2}'''.format(create_table(TREESUMMARY, T_COLS),
                         create_table(SUMMARY,     S_COLS),
                         create_table(ENTRIES,     E_COLS + [Stat('extension_len')]))

    J = '''INSERT INTO {0} SELECT {1} FROM {2};
           INSERT INTO {3} SELECT {4} FROM {5};
           INSERT INTO {6} SELECT {7} FROM {8}; '''.format(
               TREESUMMARY, ', '.join([col.name for col in T_COLS]), IT,
               SUMMARY,     ', '.join([col.name for col in S_COLS]), IS,
               ENTRIES,     ', '.join([col.name for col in E_COLS + [Stat('LENGTH(extension)')]]), IE)

    cmd = [
        args.gufi_query,
        '-n', str(args.threads),
        '-x',
        '-y', str(args.min_level),
        '-z', str(args.max_level),
        '-O', args.flatdb,
        '-I', I,
        '-T', T,
        '-S', S,
        '-E', E,
        '-K', K,
        '-J', J,
        args.index,
    ]

    if args.verbose:
        print_query(cmd)

    return run(cmd)

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

def gen_log2_cols(col, buckets):
    return gen_stat_cols(col) + gen_log2_hist_col(col, buckets)

# used to generate timestamp columns
def gen_time_cols(col, reftime):
    # duplicate min/max cols
    return gen_stat_cols(col) + [
        Stat('age_hist',   SQLITE3_TEXT, 'time_hist({0}, {1})'.format(col, reftime)),
        Stat('hour_hist',  SQLITE3_TEXT, 'category_hist(CAST({0} AS TEXT), 1)'.format(
            'strftime(\'%H\', {0})'.format(col))),
    ]

def agg(name, sql):
    TOT = 'tot'
    MIN = 'min'
    MAX = 'max'

    if name[:3] == MIN:
        return 'CAST(MIN({0}) AS {1})'.format(sql, SQLITE3_INT64)
    if name[:3] == MAX:
        return 'CAST(MAX({0}) AS {1})'.format(sql, SQLITE3_INT64)
    if name[:3] == TOT:
        return 'CAST(TOTAL({0}) AS {1})'.format(sql, SQLITE3_INT64)

    return sql

def treesummary(group_by):
    COLS = [
        'totsubdirs',
        'maxsubdirfiles',
        'maxsubdirlinks',
        'maxsubdirsize',
        'totfiles',
        'totlinks',
        'minuid',     'maxuid',
        'mingid',     'maxgid',
        'minsize',    'maxsize',
        'totzero',
        'totltk',
        'totmtk',
        'totltm',
        'totmtm',
        'totmtg',
        'totmtt',
        'totsize',
        'minctime',   'maxctime',
        'minmtime',   'maxmtime',
        'minatime',   'maxatime',
        'minblocks',  'maxblocks',
        'totxattr',
        'mincrtime',  'maxcrtime',
        'minossint1', 'maxossint1', 'totossint1',
        'minossint2', 'maxossint2', 'totossint2',
        'minossint3', 'maxossint3', 'totossint3',
        'minossint4', 'maxossint4', 'totossint4',
        # 'rectype',
        # UID,
        # GID,
    ]

    select = [
        LEVEL,
        INODE,
        PINODE,
    ]

    for col in COLS:
        select += ['{0} AS {1}'.format(agg(col, col), col)]

    return (
        'DROP TABLE IF EXISTS main.{0}; CREATE TABLE main.{0} AS {1}'.format(
            TREESUMMARY,
            build_query(select, [TREESUMMARY],
                        None, group_by)),

        build_query([LEVEL, INODE, PINODE] + ['{0} AS {1}_{0}'.format(col, TSUM)
                                              for col in COLS],
                    [TREESUMMARY]),
    )

def summary(group_by,                            # pylint: disable=too-many-locals
            reftime,
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    CONST_COLS = [
        LEVEL,
        INODE,
        PINODE,
        'fs_type',
    ]

    DIR_COLS = [
        ['name_len',
                      lambda col : gen_log2_cols(col, log2_name_len_bucket_count)],
        ['mode',      lambda col : [
                                       Stat('hist', SQLITE3_TEXT, 'mode_hist({0})'.format(col)),
                                       Stat('num_unique', sql='COUNT(DISTINCT {0})'.format(col)),
                                   ]
        ],
        ['nlink',     gen_stat_cols],
        [UID,         gen_minmax_cols],
        [GID,         gen_minmax_cols],
        ['size',      lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        ['blksize',   lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        ['blocks',    lambda col : gen_stat_cols(col)   + gen_log2_hist_col(col, log2_size_bucket_count)],
        ['atime',     lambda col : gen_time_cols(col, reftime)],
        ['mtime',     lambda col : gen_time_cols(col, reftime)],
        ['ctime',     lambda col : gen_time_cols(col, reftime)],
        ['ftime',     gen_stat_cols],
    ]

    SUM_COLS = [
        Stat('totdirs',     sql='1'),
        Stat('totsubdirs'),
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

    select = []
    alias  = []

    for col in CONST_COLS:
        select += [col]
        alias  += [col]

    for col, g in DIR_COLS:
        for g_stat in g(col):
            select += ['{0} AS {1}_{2}'.format(g_stat.sql, col, g_stat.name)]
            alias  += ['{0}_{1} AS dir_{0}_{1}'.format(col, g_stat.name)]

    for st in SUM_COLS:
        select += ['{0} AS {1}'.format(agg(st.name, st.sql), st.name)]
        alias  += [st.name]

    return (
        'DROP TABLE IF EXISTS main.{0}; CREATE TABLE main.{0} AS {1}'.format(
            SUMMARY,
            build_query(select, [SUMMARY],
                        None, group_by)),

        build_query(alias, [SUMMARY],
                    None, group_by),
        )

def entries(group_by,                            # pylint: disable=too-many-locals
            reftime,
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    UID_COLS = [
        # min, max, mode, and num_unique can all be derived from
        # hist, but computing here to have easily accesible values
        #
        # min and max are also found in SUMMARY
        Stat('min',         sql='MIN({0})'.format(UID)),
        Stat('max',         sql='MAX({0})'.format(UID)),
        Stat('mode_count',  SQLITE3_TEXT, 'mode_count(CAST({0} AS {1}))'.format(UID, SQLITE3_TEXT)),
        Stat('hist',        SQLITE3_TEXT, 'category_hist(CAST({0} AS {1}), 1)'.format(UID, SQLITE3_TEXT)),
        Stat('num_unique',  sql='COUNT(DISTINCT {0})'.format(UID)),
    ]

    GID_COLS = [
        # min, max, mode, and num_unique can all be derived from
        # hist, but computing here to have easily accesible values
        #
        # min and max are also found in SUMMARY
        Stat('min',         sql='MIN({0})'.format(GID)),
        Stat('max',         sql='MAX({0})'.format(GID)),
        Stat('mode_count',  SQLITE3_TEXT, 'mode_count(CAST({0} AS {1}))'.format(GID, SQLITE3_TEXT)),
        Stat('hist',        SQLITE3_TEXT, 'category_hist(CAST({0} AS {1}), 1)'.format(GID, SQLITE3_TEXT)),
        Stat('num_unique',  sql='COUNT(DISTINCT {0})'.format(GID)),
    ]

    SIZE_COLS = gen_log2_cols('size',                   log2_size_bucket_count) + [
        # compute total here to not need to join with SUMMARY data
        Stat('total',       sql='SUM(size)'),
    ]

    PERM_COLS = [
        # compute num_unique here to have easily accessible value
        Stat('hist',        SQLITE3_TEXT, 'mode_hist(mode)'),
        Stat('num_unique',  sql='COUNT(DISTINCT mode)'),
    ]

    ATIME_COLS  = gen_time_cols('atime',                reftime)
    MTIME_COLS  = gen_time_cols('mtime',                reftime)
    CTIME_COLS  = gen_time_cols('ctime',                reftime)
    CRTIME_COLS = gen_time_cols('crtime',               reftime)

    FTIME_COLS  = gen_stat_cols(FTIME.name)

    NAME_COLS        = gen_log2_cols('name_len',        log2_name_len_bucket_count)
    LINKNAME_COLS    = gen_log2_cols('linkname_len',    log2_name_len_bucket_count)
    # TODO: need to pull xattrs separately since there might be multiple pairs per entry # pylint: disable=fixme
    # XATTR_NAME_COLS  = gen_log2_cols('xattr_name_len',  log2_name_len_bucket_count)
    # XATTR_VALUE_COLS = gen_log2_cols('xattr_value_len', log2_name_len_bucket_count)

    EXT_COLS = [
        Stat('hist', SQLITE3_TEXT, 'category_hist(extension, 1)'),
    ]

    EXT_LEN_COLS     = gen_log2_cols('extension_len',   log2_name_len_bucket_count)

    COLS = [
        ['uid',             UID_COLS],
        ['gid',             GID_COLS],
        ['size',            SIZE_COLS],
        ['mode',            PERM_COLS],
        ['ctime',           CTIME_COLS],
        ['atime',           ATIME_COLS],
        ['mtime',           MTIME_COLS],
        ['crtime',          CRTIME_COLS],
        ['ftime',           FTIME_COLS],
        ['name_len',        NAME_COLS],
        ['linkname_len',    LINKNAME_COLS],
        # ['xattr_name_len',  XATTR_NAME_COLS],
        # ['xattr_value_len', XATTR_VALUE_COLS],
        ['extension',        EXT_COLS],
        ['extension_len',    EXT_LEN_COLS],
    ]

    select = [
        LEVEL,
        '{0} AS {1}'.format(PINODE, INODE),
        '{0} AS {1}'.format(PPINODE, PINODE)
    ]

    for col_name, sts in COLS:
        for st in sts:
            select += ['{0} AS {1}_{2}'.format(st.sql, col_name, st.name)]

    # remap GROUP BY
    ENTRIES_GROUP_BY = {
        LEVEL  : [LEVEL],
        INODE  : [PINODE],
        PINODE : [PPINODE],
    }

    entries_group_by = group_by
    if entries_group_by is not None:
        entries_group_by = ENTRIES_GROUP_BY[group_by[0]]

    return 'DROP TABLE IF EXISTS main.{0}; CREATE TABLE main.{0} AS {1}'.format(
        ENTRIES,
        build_query(select, [ENTRIES],
                    None, entries_group_by))

def snapshot(group_by, tsum_alias, sum_alias, replace):
    group_by = None if group_by is None else group_by[0]
    return '\n    '.join([
        'DROP VIEW IF EXISTS {0};'.format(SNAPSHOT) if replace else '',
        'CREATE VIEW {0}',
        'AS',
        '  SELECT *',
        '  FROM   ({3}) AS {2}',
        '         LEFT JOIN {4}',
        '                ON {2}.{1} == {4}.{1}' if group_by is not None else '',
        '         LEFT JOIN ({6}) AS {5}',
        '                ON {2}.{1} == {5}.{1}' if group_by is not None else '',
    ]).format(SNAPSHOT, group_by,
              SUM, sum_alias,
              ENTRIES,
              TSUM, tsum_alias)

def create_longitudinal_snapshot(args):
    group_by = GROUP_BY[args.view]
    log2_size_bucket_count = int(math.ceil(math.log(args.max_size, 2)))
    log2_name_len_bucket_count = int(math.ceil(math.log(args.max_name_len, 2)))

    tsum_stat, tsum_alias = treesummary(group_by)

    sum_stat,  sum_alias  = summary(group_by,
                                    args.reftime,
                                    log2_size_bucket_count,
                                    log2_name_len_bucket_count)

    ent_stat              = entries(group_by,
                                    args.reftime,
                                    log2_size_bucket_count,
                                    log2_name_len_bucket_count)

    cmd = [
        '@CMAKE_BINARY_DIR@/contrib/gufi_sqlite',

        args.outdb,

        'ATTACH \'file:{0}?mode=ro\' AS {1};'.format(args.flatdb, FLATDB),

        tsum_stat,

        sum_stat,

        ent_stat,

        snapshot(group_by, tsum_alias, sum_alias, args.replace_outdb),
    ]

    if args.verbose:
        print('{0} \\'.format(cmd[0]))
        print('    "{0}" \\'.format(cmd[1]))
        for argv in cmd[2:-1]:
            print('    "{0}" \\'.format(argv))
        print('    "{0}"'.format(cmd[-1]))

    return run(cmd)

def write_metadata(args, conn, timestamp):
    if args.replace_outdb:
        conn.execute('DROP TABLE IF EXISTS {0};'.format(METADATA))

    notes = args.notes
    if notes:
        notes = '\'{0}\''.format(notes)
    else:
        notes = SQLITE3_NULL

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
        VALUES ({1}, \'{2}\', {3});
    '''.format(METADATA, timestamp, args.index, notes))

def main(argv): # pylint: disable=too-many-locals
    timestamp = int(time.time())

    args = parse_args(argv, timestamp)

    # collect raw data from index
    rc = flatten_index(args)
    if rc != 0:
        return rc

    # compute stats on raw data
    rc = create_longitudinal_snapshot(args)
    if rc != 0:
        return rc

    # tack on user data
    with sqlite3.connect(args.outdb) as conn:
        write_metadata(args, conn, timestamp)

    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
