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

from gufi_common import build_query, get_positive, print_query, VRXPENTRIES, SUMMARY, VRXSUMMARY, TREESUMMARY

METADATA       = 'metadata' # user data
SNAPSHOT       = 'snapshot' # SUMMARY left joined with optional TREESUMMARY

INODE          = 'inode'
PINODE         = 'pinode'

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

def gen_value_hist_col(col):
    return ['category_hist(CAST({0} AS TEXT), 1)'.format(col), SQLITE3_TEXT]

# used to generate timestamp columns
def gen_time_cols(col, reftime):
    return [
        ['min',       ['{{0}}.dmin{0}'.format(col),                                 SQLITE3_INT64]],
        ['max',       ['{{0}}.dmax{0}'.format(col),                                 SQLITE3_INT64]],
        ['mean',      ['AVG({{0}}.{0})'.format(col),                                SQLITE3_DOUBLE]],
        ['median',    ['median({{0}}.{0})'.format(col),                             SQLITE3_DOUBLE]],
        ['mode',      ['mode_count({{0}}.{0})'.format(col),                         SQLITE3_TEXT]],
        ['stdev',     ['stdevp({{0}}.{0})'.format(col),                             SQLITE3_DOUBLE]],
        ['age_hist',  ['time_hist({{0}}.{0}, {1})'.format(col, reftime),            SQLITE3_TEXT]],
        ['hour_hist', gen_value_hist_col('strftime(\'%H\', {{0}}.{0})'.format(col))],
    ]

# used to generate columns for name, linkname, xattr_name, and xattr_value
def gen_str_cols(col, buckets):
    return [
        ['min',       ['MIN(LENGTH({{0}}.{0}))'.format(col),                        SQLITE3_INT64]],
        ['max',       ['MAX(LENGTH({{0}}.{0}))'.format(col),                        SQLITE3_INT64]],
        ['mean',      ['AVG(LENGTH({{0}}.{0}))'.format(col),                        SQLITE3_DOUBLE]],
        ['median',    ['median(LENGTH({{0}}.{0}))'.format(col),                     SQLITE3_DOUBLE]],
        ['mode',      ['mode_count(LENGTH({{0}}.{0}))'.format(col),                 SQLITE3_TEXT]],
        ['stdev',     ['stdevp(LENGTH({{0}}.{0}))'.format(col),                     SQLITE3_DOUBLE]],
        ['hist',      ['log2_hist(LENGTH({{0}}.{0}), {1})'.format(col, buckets),    SQLITE3_TEXT]],
    ]

def treesummary():
    # copied from dbutils.h
    TREESUMMARY_CREATE = 'CREATE TABLE {0} (inode TEXT, totsubdirs INT64, maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totzero INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, uid INT64, gid INT64)'

    INTERMEDIATE = 'intermediate_treesummary'

    return (
        INTERMEDIATE,
        TREESUMMARY_CREATE.format(INTERMEDIATE),
        TREESUMMARY_CREATE.format(TREESUMMARY),
    )

# pylint: disable=too-many-locals
def summary(reftime,
            log2_size_bucket_count,
            log2_name_len_bucket_count):
    SUMMARY_COLS = [
        ['name',            ['basename({0}.name)',                   SQLITE3_TEXT]],
        [INODE,             ['{{0}}.{0}'.format(INODE),              SQLITE3_TEXT]],
        ['mode',            ['{0}.mode',                             SQLITE3_INT64]],
        ['nlink',           ['{0}.nlink',                            SQLITE3_INT64]],
        ['uid',             ['{0}.uid',                              SQLITE3_INT64]],
        ['gid',             ['{0}.gid',                              SQLITE3_INT64]],
        ['blksize',         ['{0}.blksize',                          SQLITE3_INT64]],
        ['blocks',          ['{0}.blocks',                           SQLITE3_INT64]],
        ['atime',           ['{0}.atime',                            SQLITE3_INT64]],
        ['mtime',           ['{0}.mtime',                            SQLITE3_INT64]],
        ['ctime',           ['{0}.ctime',                            SQLITE3_INT64]],
        ['depth',           ['level()',                              SQLITE3_INT64]],
        ['filesystem_type', [SQLITE3_NULL,                           SQLITE3_BLOB]],
        [PINODE,            ['{{0}}.{0}'.format(PINODE),             SQLITE3_TEXT]],
        ['totfiles',        ['{0}.totfiles',                         SQLITE3_INT64]],
        ['totlinks',        ['{0}.totlinks',                         SQLITE3_INT64]],
        ['totsubdirs',      ['subdirs({0}.srollsubdirs, {0}.sroll)', SQLITE3_INT64]],
    ]

    UID_COLS = [
        ['min',             ['{0}.dminuid',                          SQLITE3_INT64]],
        ['max',             ['{0}.dmaxuid',                          SQLITE3_INT64]],
        ['hist',            gen_value_hist_col('{0}.uid')],
        ['num_unique',      ['COUNT(DISTINCT {0}.uid)',              SQLITE3_INT64]],
    ]

    GID_COLS = [
        ['min',             ['{0}.dmingid',                          SQLITE3_INT64]],
        ['max',             ['{0}.dmaxgid',                          SQLITE3_INT64]],
        ['hist',            gen_value_hist_col('{0}.gid')],
        ['num_unique',      ['COUNT(DISTINCT {0}.gid)',              SQLITE3_INT64]],
    ]

    SIZE_COLS = [
        ['min',             ['{0}.dminsize',                         SQLITE3_INT64]],
        ['max',             ['{0}.dmaxsize',                         SQLITE3_INT64]],
        ['mean',            ['AVG({0}.size)',                        SQLITE3_DOUBLE]],
        ['median',          ['median({0}.size)',                     SQLITE3_DOUBLE]],
        ['mode',            ['mode_count(CAST({0}.size AS TEXT))',   SQLITE3_TEXT]],
        ['stdev',           ['stdevp({0}.size)',                     SQLITE3_DOUBLE]],
        ['sum',             ['{0}.dtotsize',                         SQLITE3_INT64]],
        ['hist',            ['log2_hist({{0}}.size, {0})'.format(log2_size_bucket_count),
                                                                     SQLITE3_TEXT]],
    ]

    PERM_COLS = [
        ['hist',            ['mode_hist({0}.mode)',                  SQLITE3_TEXT]],
    ]

    ATIME_COLS       = gen_time_cols('atime',      reftime)
    MTIME_COLS       = gen_time_cols('mtime',      reftime)
    CTIME_COLS       = gen_time_cols('ctime',      reftime)
    CRTIME_COLS      = gen_time_cols('crtime',     reftime)

    NAME_COLS        = gen_str_cols('name',        log2_name_len_bucket_count)
    LINKNAME_COLS    = gen_str_cols('linkname',    log2_name_len_bucket_count)
    XATTR_NAME_COLS  = gen_str_cols('xattr_name',  log2_name_len_bucket_count)
    XATTR_VALUE_COLS = gen_str_cols('xattr_value', log2_name_len_bucket_count)

    EXT_COLS = [
        # filenames without extensions pass in NULL
        ['hist', ['''category_hist(CASE WHEN {{0}}.name NOT LIKE '%.%' THEN
                                       {0}
                                   ELSE
                                       REPLACE({{0}}.name, RTRIM({{0}}.name, REPLACE({{0}}.name, '.', '')), '')
                                   END, 1)'''.format(SQLITE3_NULL),
                 SQLITE3_TEXT]]
    ]

    ENTRIES_COLS = [
        ['uid',         UID_COLS],
        ['gid',         GID_COLS],
        ['size',        SIZE_COLS],
        ['permissions', PERM_COLS],
        ['ctime',       CTIME_COLS],
        ['atime',       ATIME_COLS],
        ['mtime',       MTIME_COLS],
        ['crtime',      CRTIME_COLS],
        ['name',        NAME_COLS],
        ['linkname',    LINKNAME_COLS],
        ['xattr_name',  XATTR_NAME_COLS],
        ['xattr_value', XATTR_VALUE_COLS],
        ['extensions',  EXT_COLS],
    ]

    # generate SUMMARY columns for selecting and inserting into

    IK = [] # used for creating intermediate tables and aggregating
    E  = [] # SELECT cols FROM VRXSUMMARY LEFT JOIN VRXPENTRIES ON VRXSUMMARY.inode == VRXPENTRIES.pinode

    for col_name, stat in SUMMARY_COLS:
        sql, col_type = stat
        IK += ['{0} {1}'.format(col_name, col_type)]
        E  += [sql.format(VRXSUMMARY)]

    for col_name, stats_pulled in ENTRIES_COLS:
        for stat_name, stat in stats_pulled:
            sql, col_type = stat
            IK += ['{0}_{1} {2}'.format(col_name, stat_name, col_type)]
            E  += [sql.format(VRXPENTRIES)]

    # same for intermediate and aggregate
    SUMMARY_CREATE = 'CREATE TABLE {{0}}({0})'.format(', '.join(IK))

    INTERMEDIATE = 'intermediate_summary'

    return (
        INTERMEDIATE,
        SUMMARY_CREATE.format(INTERMEDIATE),
        SUMMARY_CREATE.format(SUMMARY),
        E
    )

def run(argv):
    timestamp = int(time.time())

    args = parse_args(argv, timestamp)

    log2_size_bucket_count = int(math.ceil(math.log(args.max_size, 2)))
    log2_name_len_bucket_count = int(math.ceil(math.log(args.max_name_len, 2)))

    # ############################################################################
    # get SQL strings for constructing command
    ts_int, ts_I, ts_K = treesummary()
    sum_int, sum_I, sum_K, sum_cols = summary(args.reftime,
                                              log2_size_bucket_count,
                                              log2_name_len_bucket_count)
    # ############################################################################

    # construct full command to run

    # ############################################################################
    I = '{0}; {1};'.format(sum_I, ts_I)
    # ############################################################################

    # ############################################################################
    # if TREESUMMARY tables are present in the index, this table
    # will likely have 1 more row than the snapshot table due to
    # pulling the row from the top-level db that should otherwise
    # be empty
    T = 'INSERT INTO {0} SELECT * FROM {1}; SELECT 1;'.format(ts_int, TREESUMMARY)
    # ############################################################################

    # ############################################################################
    # Have to left join here to keep at least 1 record if there are no entries
    #
    # This has the side effect of getting rid of the the top-level entry if
    # the path passed into this script was root of multiple indexes instead of
    # the root of a single index because the common parent of multiple indexes
    # should not have anything in the summary table.
    E = 'INSERT INTO {0} {1};'.format(
        sum_int, build_query(sum_cols,
                             ['{0} LEFT JOIN {1} ON {0}.{2} == {1}.{3}'.format(
                                 VRXSUMMARY, VRXPENTRIES, INODE, PINODE)],
                             None,
                             ['{0}.{1}'.format(VRXSUMMARY, INODE)]))
    # ############################################################################

    # ############################################################################
    K = '{0}; {1};'.format(ts_K, sum_K)

    if args.replace:
        K = '''
            DROP TABLE IF EXISTS {0};
            DROP TABLE IF EXISTS {1};
            {2}
            '''.format(TREESUMMARY, SUMMARY, K)
    # ############################################################################

    # ############################################################################
    J = '''
        INSERT INTO {0} SELECT * FROM {1};
        INSERT INTO {2} SELECT * FROM {3};
        '''.format(TREESUMMARY, ts_int, SUMMARY, sum_int)
    # ############################################################################

    # ############################################################################
    G = '''
        CREATE VIEW {0}
        AS
          SELECT *
          FROM   {2}
                 LEFT JOIN {3}
                        ON {2}.{1} == {3}.{1};
        '''.format(SNAPSHOT, INODE, SUMMARY, TREESUMMARY)

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
