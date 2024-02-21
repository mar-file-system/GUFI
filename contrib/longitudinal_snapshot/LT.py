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

from gufi_common import build_query, get_positive, print_query, ENTRIES, VRXPENTRIES, SUMMARY, VRXSUMMARY, TREESUMMARY

METADATA       = 'metadata' # user data
SNAPSHOT       = 'snapshot' # SUMMARY left joined with optional ENTRIES and TREESUMMARY data

INODE          = 'inode'
PINODE         = 'pinode'
DEPTH          = 'depth'

SQLITE3_NULL   = 'NULL'
SQLITE3_INT64  = 'INT64'
SQLITE3_DOUBLE = 'DOUBLE'
SQLITE3_TEXT   = 'TEXT'
SQLITE3_BLOB   = 'BLOB'

def parse_args(argv, now):
    parser = argparse.ArgumentParser(description='GUFI Longitudinal Snapshot Generator')
    parser.add_argument('--verbose', '-V', action='store_true',
                        help='Show the gufi_query being executed')
    parser.add_argument('index',
                        help='index to snapshot')
    parser.add_argument('outname',
                        help='output db file name')
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

    parser.add_argument('--gufi_query',    metavar='path',    type=str,           default='gufi_query',
                        help='path to gufi_query executable')
    parser.add_argument('--threads', '-n', metavar='count',   type=get_positive,  default=1,
                        help='thread count')

    return parser.parse_args(argv[1:])

def create_table(name, cols, temp):
    return 'CREATE {0} TABLE {1} ({2})'.format(
        'TEMP' if temp else '',
        name,
        ', '.join(cols)
    )

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

    raise NameError('Unknown column: {0}'.format(col))

def treesummary():
    # copied from dbutils.h
    COLS = [
        # ['inode',              SQLITE3_TEXT],
        ['totsubdirs',         SQLITE3_INT64],
        ['maxsubdirfiles',     SQLITE3_INT64],
        ['maxsubdirlinks',     SQLITE3_INT64],
        ['maxsubdirsize',      SQLITE3_INT64],
        ['totfiles',           SQLITE3_INT64],
        ['totlinks',           SQLITE3_INT64],
        ['minuid',             SQLITE3_INT64],
        ['maxuid',             SQLITE3_INT64],
        ['mingid',             SQLITE3_INT64],
        ['maxgid',             SQLITE3_INT64],
        ['minsize',            SQLITE3_INT64],
        ['maxsize',            SQLITE3_INT64],
        ['totzero',            SQLITE3_INT64],
        ['totltk',             SQLITE3_INT64],
        ['totmtk',             SQLITE3_INT64],
        ['totltm',             SQLITE3_INT64],
        ['totmtm',             SQLITE3_INT64],
        ['totmtg',             SQLITE3_INT64],
        ['totmtt',             SQLITE3_INT64],
        ['totsize',            SQLITE3_INT64],
        ['minctime',           SQLITE3_INT64],
        ['maxctime',           SQLITE3_INT64],
        ['minmtime',           SQLITE3_INT64],
        ['maxmtime',           SQLITE3_INT64],
        ['minatime',           SQLITE3_INT64],
        ['maxatime',           SQLITE3_INT64],
        ['minblocks',          SQLITE3_INT64],
        ['maxblocks',          SQLITE3_INT64],
        ['totxattr',           SQLITE3_INT64],
        # [DEPTH,                SQLITE3_INT64],
        ['mincrtime',          SQLITE3_INT64],
        ['maxcrtime',          SQLITE3_INT64],
        ['minossint1',         SQLITE3_INT64],
        ['maxossint1',         SQLITE3_INT64],
        ['totossint1',         SQLITE3_INT64],
        ['minossint2',         SQLITE3_INT64],
        ['maxossint2',         SQLITE3_INT64],
        ['totossint2',         SQLITE3_INT64],
        ['minossint3',         SQLITE3_INT64],
        ['maxossint3',         SQLITE3_INT64],
        ['totossint3',         SQLITE3_INT64],
        ['minossint4',         SQLITE3_INT64],
        ['maxossint4',         SQLITE3_INT64],
        ['totossint4',         SQLITE3_INT64],
        # ['rectype',            SQLITE3_INT64],
        # ['uid',                SQLITE3_INT64],
        # ['gid',                SQLITE3_INT64],
    ]

    # -I
    I = [
        '{0} {1}'.format(DEPTH, SQLITE3_INT64),
    ]

    # -T
    T = [
        'level()',
    ]

    # -K
    K = [
        '{0} {1}'.format(DEPTH, SQLITE3_INT64),
    ]

    # -J and -G
    JG = [
        DEPTH,
    ]

    # rename columns for SNAPSHOT
    V = [
        '{0} AS ts_{0}'.format(col_name)
        for col_name, _ in [[DEPTH, None]] + COLS
    ]

    for col_name, col_type in COLS:
        I  += ['{0} {1}'.format(col_name, col_type)]
        T  += [col_name]                              # pull raw data
        K  += ['{0} {1}'.format(col_name, col_type)]
        JG += [agg(col_name)]                         # aggregate data

    INTERMEDIATE = 'intermediate_treesummary'

    J_name = 'J_treesummary' # holds per thread results

    return (
        # -I
        create_table(INTERMEDIATE, I, False),

        # -T
        'INSERT INTO {0} {1}; SELECT 1'.format(INTERMEDIATE,
                                               build_query(T, [TREESUMMARY])),

        # -K
        '{0}; {1}'.format(create_table(J_name,      I, True),
                          create_table(TREESUMMARY, K, False)),

        # -J
        'INSERT INTO {0} {1}'.format(J_name,
                                     build_query(JG, [INTERMEDIATE],
                                                 None, [DEPTH])),

        # -G
        'INSERT INTO {0} {1}; DROP TABLE {2}'.format(TREESUMMARY,
                                                     build_query(JG, [J_name],
                                                                 None, [DEPTH]),
                                                     J_name),

        # rename columns for SNAPSHOT
        build_query(V, [TREESUMMARY])
    )

def summary():
    COLS = [
        # not keeping stats of directory itself
        ['totfiles',       ['totfiles',                         SQLITE3_INT64]],
        ['totlinks',       ['totlinks',                         SQLITE3_INT64]],
        ['totsubdirs',     ['subdirs(srollsubdirs, sroll)',     SQLITE3_INT64]],
        ['minuid',         ['minuid',                           SQLITE3_INT64]],
        ['maxuid',         ['maxuid',                           SQLITE3_INT64]],
        ['mingid',         ['mingid',                           SQLITE3_INT64]],
        ['maxgid',         ['maxgid',                           SQLITE3_INT64]],
        ['minsize',        ['minsize',                          SQLITE3_INT64]],
        ['maxsize',        ['maxsize',                          SQLITE3_INT64]],
        ['totsize',        ['totsize',                          SQLITE3_INT64]],
        ['minossint1',     ['minossint1',                       SQLITE3_INT64]],
        ['maxossint1',     ['maxossint1',                       SQLITE3_INT64]],
        ['totossint1',     ['totossint1',                       SQLITE3_INT64]],
        ['minossint2',     ['minossint2',                       SQLITE3_INT64]],
        ['maxossint2',     ['maxossint2',                       SQLITE3_INT64]],
        ['totossint2',     ['totossint2',                       SQLITE3_INT64]],
        ['minossint3',     ['minossint3',                       SQLITE3_INT64]],
        ['maxossint3',     ['maxossint3',                       SQLITE3_INT64]],
        ['totossint3',     ['totossint3',                       SQLITE3_INT64]],
        ['minossint4',     ['minossint4',                       SQLITE3_INT64]],
        ['maxossint4',     ['maxossint4',                       SQLITE3_INT64]],
        ['totossint4',     ['totossint4',                       SQLITE3_INT64]],
    ]

    # -I and -K
    IK = [
        '{0} {1}'.format(DEPTH, SQLITE3_INT64),
    ]

    # -S
    S = [
        'level()',
    ]

    # -J and -G
    JG = [
        DEPTH,
    ]

    for col_name, props in COLS:
        sql, col_type = props
        IK += ['{0} {1}'.format(col_name, col_type)]
        S  += [sql]             # pull raw data
        JG += [agg(col_name)]   # aggregate data

    INTERMEDIATE = 'intermediate_summary'

    J_name = 'J_summary' # holds per thread results

    return (
        # -I
        create_table(INTERMEDIATE, IK, False),

        # -S
        'INSERT INTO {0} {1}; SELECT 1'.format(INTERMEDIATE,
                                               build_query(S, [VRXSUMMARY],
                                                           None, [INODE])),

        # -K
        '{0}; {1}'.format(create_table(J_name,  IK, True),
                          create_table(SUMMARY, IK, False)),

        # -J
        'INSERT INTO {0} {1};'.format(J_name,
                                     build_query(JG, [INTERMEDIATE],
                                                 None, [DEPTH])),

        # -G
        'INSERT INTO {0} {1}; DROP TABLE {2}'.format(SUMMARY,
                                                     build_query(JG, [J_name],
                                                                 None, [DEPTH]),
                                                     J_name),
    )

def gen_value_hist_col(col):
    return ['category_hist(CAST({0} AS TEXT), 1)'.format(col), SQLITE3_TEXT]

# used to generate timestamp columns
def gen_time_cols(col, reftime):
    return [
        # min/max already captured by SUMMARY
        ['mean',      ['AVG({0})'.format(col),                                SQLITE3_DOUBLE]],
        ['median',    ['median({0})'.format(col),                             SQLITE3_DOUBLE]],
        ['mode',      ['mode_count({0})'.format(col),                         SQLITE3_TEXT]],
        ['stdev',     ['stdevp({0})'.format(col),                             SQLITE3_DOUBLE]],
        ['age_hist',  ['time_hist({0}, {1})'.format(col, reftime),            SQLITE3_TEXT]],
        ['hour_hist', gen_value_hist_col('strftime(\'%H\', {0})'.format(col))],
    ]

# used to generate columns for name, linkname, xattr_name, and xattr_value
def gen_str_cols(col, buckets):
    return [
        ['min',       ['MIN({0})'.format(col),                       SQLITE3_INT64]],
        ['max',       ['MAX({0})'.format(col),                       SQLITE3_INT64]],
        ['mean',      ['AVG({0})'.format(col),                       SQLITE3_DOUBLE]],
        ['median',    ['median({0})'.format(col),                    SQLITE3_DOUBLE]],
        ['mode',      ['mode_count({0})'.format(col),                SQLITE3_TEXT]],
        ['stdev',     ['stdevp({0})'.format(col),                    SQLITE3_DOUBLE]],
        ['hist',      ['log2_hist({0}, {1})'.format(col, buckets),   SQLITE3_TEXT]],
    ]

def entries(reftime,                           # pylint: disable=too-many-locals
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    # -E
    E_COLS = [
        ['name_len',         ['LENGTH(name)',        SQLITE3_INT64]],
        # type and inode are already captured by SUMMARY
        ['mode',             ['mode',                SQLITE3_INT64]],
        ['nlink',            ['nlink',               SQLITE3_INT64]],
        ['uid',              ['uid',                 SQLITE3_INT64]],
        ['gid',              ['gid',                 SQLITE3_INT64]],
        ['size',             ['''
                              CASE WHEN type == \'f\' THEN
                                  size
                              ELSE
                                  {0}
                              END'''.format(SQLITE3_NULL),
                                                     SQLITE3_INT64]],
        ['blksize',          ['blksize',             SQLITE3_INT64]],
        ['blocks',           ['blocks',              SQLITE3_INT64]],
        ['atime',            ['atime',               SQLITE3_INT64]],
        ['mtime',            ['mtime',               SQLITE3_INT64]],
        ['ctime',            ['ctime',               SQLITE3_INT64]],
        ['linkname_len',     ['''
                              CASE WHEN type == \'l\' THEN
                                  LENGTH(linkname)
                              ELSE
                                  {0}
                              END
                              '''.format(SQLITE3_NULL),
                                                     SQLITE3_INT64]],
        # ['xattr_name_len',   ['LENGTH(xattr_name)',  SQLITE3_INT64]],
        # ['xattr_value_len',  ['LENGTH(xattr_value)', SQLITE3_INT64]],
        ['crtime',           ['crtime',              SQLITE3_INT64]],
        ['ossint1',          ['ossint1',             SQLITE3_INT64]],
        ['ossint2',          ['ossint2',             SQLITE3_INT64]],
        ['ossint3',          ['ossint3',             SQLITE3_INT64]],
        ['ossint4',          ['ossint4',             SQLITE3_INT64]],
        ['osstext1_len',     ['LENGTH(osstext1)',    SQLITE3_INT64]],
        ['osstext2_len',     ['LENGTH(osstext2)',    SQLITE3_INT64]],
        # pinode and ppinode are not useful

        # filenames without extensions pass in NULL
        ['extension',        ['''
                              CASE WHEN name NOT LIKE '%.%' THEN
                                  {0}
                              ELSE
                                  REPLACE(name, RTRIM(name, REPLACE(name, '.', '')), '')
                              END
                              '''.format(SQLITE3_NULL),
                                                     SQLITE3_TEXT]],
    ]

    # -G

    UID_COLS = [
        # min/max already captured by SUMMARY
        ['hist',            gen_value_hist_col('uid')],
        # use hist to get num_unique
    ]

    GID_COLS = [
        # min/max already captured by SUMMARY
        ['hist',            gen_value_hist_col('gid')],
        # use hist to get num_unique
    ]

    SIZE_COLS = [
        # min/max already captured by SUMMARY
        ['mean',            ['AVG(size)',                        SQLITE3_DOUBLE]],
        ['median',          ['median(size)',                     SQLITE3_DOUBLE]],
        ['mode',            ['mode_count(CAST(size AS TEXT))',   SQLITE3_TEXT]],
        ['stdev',           ['stdevp(size)',                     SQLITE3_DOUBLE]],
        # sum already captured by SUMMARY
        ['hist',            ['log2_hist(size, {0})'.format(log2_size_bucket_count),
                                                                 SQLITE3_TEXT]],
    ]

    PERM_COLS = [
        ['hist',            ['mode_hist(mode)',                  SQLITE3_TEXT]],
    ]

    ATIME_COLS       = gen_time_cols('atime',                    reftime)
    MTIME_COLS       = gen_time_cols('mtime',                    reftime)
    CTIME_COLS       = gen_time_cols('ctime',                    reftime)
    CRTIME_COLS      = gen_time_cols('crtime',                   reftime)

    NAME_COLS        = gen_str_cols('name_len',                  log2_name_len_bucket_count)
    LINKNAME_COLS    = gen_str_cols('linkname_len',              log2_name_len_bucket_count)
    # TODO: need to pull xattrs separately since there might be multiple pairs per entry # pylint: disable=fixme
    # XATTR_NAME_COLS  = gen_str_cols('xattr_name_len',            log2_name_len_bucket_count)
    # XATTR_VALUE_COLS = gen_str_cols('xattr_value_len',           log2_name_len_bucket_count)

    EXT_COLS = [
        ['hist',       ['category_hist(extension, 1)',           SQLITE3_TEXT]],
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
        ['name_len',        NAME_COLS],
        ['linkname_len',    LINKNAME_COLS],
        # ['xattr_name_len',  XATTR_NAME_COLS],
        # ['xattr_value_len', XATTR_VALUE_COLS],
        ['extensions',      EXT_COLS],
        ['extensions_len',  EXT_LEN_COLS],
    ]

    # -I
    I = [
        '{0} {1}'.format(DEPTH, SQLITE3_INT64),
    ]

    # -E
    E = [
        'level()',
    ]

    # -K
    K = [
        '{0} {1}'.format(DEPTH, SQLITE3_INT64),
    ]

    # -G
    G = [
        DEPTH,
    ]

    for col_name, props in E_COLS:
        sql, col_type = props
        I += ['{0} {1}'.format(col_name, col_type)]
        E += [sql]             # pull raw data

    for col_name, stats_pulled in G_COLS:
        for stat_name, props in stats_pulled:
            sql, col_type = props
            K += ['{0}_{1} {2}'.format(col_name, stat_name, col_type)]
            G += [sql]         # aggregate data

    INTERMEDIATE = 'intermediate_entries'

    J_name = 'J_entries' # holds per thread results

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
                                                                 None, [DEPTH]),
                                                     J_name),
    )

def run(argv): # pylint: disable=too-many-locals
    timestamp = int(time.time())

    args = parse_args(argv, timestamp)

    log2_size_bucket_count = int(math.ceil(math.log(args.max_size, 2)))
    log2_name_len_bucket_count = int(math.ceil(math.log(args.max_name_len, 2)))

    # ############################################################################
    # get SQL strings for constructing command
    ts_I, T, ts_K, ts_J, ts_G, ts_V = treesummary()
    sum_I, S, sum_K, sum_J, sum_G = summary()
    ent_I, E, ent_K, ent_J, ent_G = entries(args.reftime,
                                            log2_size_bucket_count,
                                            log2_name_len_bucket_count)
    # ############################################################################

    # construct full command to run

    # ############################################################################
    I = '{0}; {1}; {2};'.format(ts_I, sum_I, ent_I)
    # ############################################################################

    # ############################################################################
    K = '{0}; {1}; {2};'.format(ts_K, sum_K, ent_K)

    if args.replace:
        K = '''
            DROP TABLE IF EXISTS {0};
            DROP TABLE IF EXISTS {1};
            DROP TABLE IF EXISTS {2};
            {3}
            '''.format(TREESUMMARY, SUMMARY, ENTRIES, K)
    ############################################################################

    # ############################################################################
    J = '{0}; {1}; {2};'.format(ts_J, sum_J, ent_J)
    # ############################################################################

    # ############################################################################
    G = '''
        {0};
        {1};
        {2};
        CREATE VIEW {3}
        AS
          SELECT *
          FROM   {5}
                 LEFT JOIN {6}
                        ON {5}.{4} == {6}.{4}
                 LEFT JOIN ({7}) AS ts
                        ON {5}.{4} == ts.ts_{4};
        '''.format(ts_G, sum_G, ent_G,
                   SNAPSHOT, DEPTH, SUMMARY, ENTRIES, ts_V)

    if args.replace:
        G = 'DROP VIEW IF EXISTS {0}; {1}'.format(SNAPSHOT, G)
    # ############################################################################

    cmd = [
        args.gufi_query,
        args.index,
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
