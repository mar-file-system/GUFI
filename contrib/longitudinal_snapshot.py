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

from gufi_common import build_query, get_positive, VRXPENTRIES, TREESUMMARY
import gufi_config

METADATA       = 'metadata'
SNAPSHOT       = 'snapshot'

SQLITE3_NULL   = 'NULL'
SQLITE3_INT64  = 'INT64'
SQLITE3_DOUBLE = 'DOUBLE'
SQLITE3_TEXT   = 'TEXT'
SQLITE3_BLOB   = 'BLOB'

# used to generate timestamp columns
def gen_time_cols(col, reftime):
    return [
        ['min',    ['dmin{0}'.format(col),                      SQLITE3_INT64]],
        ['max',    ['dmax{0}'.format(col),                      SQLITE3_INT64]],
        ['mean',   ['AVG({0})'.format(col),                     SQLITE3_DOUBLE]],
        ['median', ['median({0})'.format(col),                  SQLITE3_DOUBLE]],
        ['mode',   ['mode_count({0})'.format(col),              SQLITE3_TEXT]],
        ['stdev',  ['stdevp({0})'.format(col),                  SQLITE3_DOUBLE]],
        ['hist',   ['time_hist({0}, {1})'.format(col, reftime), SQLITE3_TEXT]],
    ]

# used to generate columns for name, linkname, xattr_name, and xattr_value
def gen_str_cols(col, buckets):
    return [
        ['min',    ['MIN(LENGTH({0}))'.format(col),                     SQLITE3_INT64]],
        ['max',    ['MAX(LENGTH({0}))'.format(col),                     SQLITE3_INT64]],
        ['mean',   ['AVG(LENGTH({0}))'.format(col),                     SQLITE3_DOUBLE]],
        ['median', ['median(LENGTH({0}))'.format(col),                  SQLITE3_DOUBLE]],
        ['mode',   ['mode_count(LENGTH({0}))'.format(col),              SQLITE3_TEXT]],
        ['stdev',  ['stdevp(LENGTH({0}))'.format(col),                  SQLITE3_DOUBLE]],
        ['hist',   ['log2_hist(LENGTH({0}), {1})'.format(col, buckets), SQLITE3_TEXT]],
    ]

def parse_args(argv, now):
    parser = argparse.ArgumentParser(description='GUFI Longitudinal Snapshot Generator')
    parser.add_argument('outname',
                        help='output db file name')
    parser.add_argument('--reftime',      metavar='seconds', type=int,           default=now,
                        help='reference point for age (since UNIX epoch)')
    parser.add_argument('--max_size',     metavar='pos_int', type=get_positive,  default=1 << 50,
                        help='the maximum expected size')
    parser.add_argument('--max_name_len', metavar='pos_int', type=get_positive,  default=1 << 8,
                        help='the maximum expected length of a name/linkname')
    parser.add_argument('--notes',        metavar='text',    type=str,           default=None,
                        help='freeform text of any extra information to add to the snapshot')

    return parser.parse_args(argv[1:])

# pylint: disable=too-many-locals, too-many-statements
def run(argv):
    timestamp = int(time.time())

    args = parse_args(argv, timestamp)
    config = gufi_config.Server(gufi_config.PATH)

    log2_size_bucket_count = math.ceil(math.log(args.max_size, 2))
    log2_name_len_bucket_count = math.ceil(math.log(args.max_name_len, 2))

    # ###############################################################
    # the following structs contain mappings from the GUFI tree to the longitudinal snapshot

    # map VRXPENTRIES columns back to SUMMARY column names
    SUMMARY_COLS = [
        ['name',            ['dname',                        SQLITE3_TEXT]],
        ['inode',           ['pinode',                       SQLITE3_TEXT]], # entry's pinode is directory's inode
        ['mode',            ['dmode',                        SQLITE3_INT64]],
        ['nlink',           ['dnlink',                       SQLITE3_INT64]],
        ['uid',             ['duid',                         SQLITE3_INT64]],
        ['gid',             ['dgid',                         SQLITE3_INT64]],
        ['blksize',         ['dblksize',                     SQLITE3_INT64]],
        ['blocks',          ['dblocks',                      SQLITE3_INT64]],
        ['atime',           ['datime',                       SQLITE3_INT64]],
        ['mtime',           ['dmtime',                       SQLITE3_INT64]],
        ['ctime',           ['dctime',                       SQLITE3_INT64]],
        ['depth',           ['level()',                      SQLITE3_INT64]],
        ['filesystem_type', [SQLITE3_NULL,                   SQLITE3_BLOB]],
        ['pinode',          ['ppinode',                      SQLITE3_TEXT]], # entry's ppinode is directory's pinode
        ['totfiles',        ['dtotfile',                     SQLITE3_INT64]],
        ['totlinks',        ['dtotlinks',                    SQLITE3_INT64]],
        ['totsubdirs',      ['subdirs(srollsubdirs, sroll)', SQLITE3_INT64]],
    ]

    UID_COLS = [
        ['min',        ['dminuid',             SQLITE3_INT64]],
        ['max',        ['dmaxuid',             SQLITE3_INT64]],
        ['hist',       ['category_hist(uid)',  SQLITE3_INT64]],
        ['num_unique', ['COUNT(DISTINCT uid)', SQLITE3_INT64]],
    ]

    GID_COLS = [
        ['min',        ['dmingid',             SQLITE3_INT64]],
        ['max',        ['dmaxgid',             SQLITE3_INT64]],
        ['hist',       ['category_hist(gid)',  SQLITE3_TEXT]],
        ['num_unique', ['COUNT(DISTINCT gid)', SQLITE3_INT64]],
    ]

    SIZE_COLS = [
        ['min',    ['dminsize',                                            SQLITE3_INT64]],
        ['max',    ['dmaxsize',                                            SQLITE3_INT64]],
        ['mean',   ['AVG(size)',                                           SQLITE3_DOUBLE]],
        ['median', ['median(size)',                                        SQLITE3_DOUBLE]],
        ['mode',   ['mode_count(CAST(size AS TEXT))',                      SQLITE3_TEXT]],
        ['stdev',  ['stdevp(size)',                                        SQLITE3_DOUBLE]],
        ['sum',    ['dtotsize',                                            SQLITE3_INT64]],
        ['hist',   ['log2_hist(size, {0})'.format(log2_size_bucket_count), SQLITE3_TEXT]],
    ]

    PERM_COLS = [
        ['hist', ['mode_hist(mode)', SQLITE3_TEXT]],
    ]

    CTIME_COLS       = gen_time_cols('ctime',  args.reftime)
    ATIME_COLS       = gen_time_cols('atime',  args.reftime)
    MTIME_COLS       = gen_time_cols('mtime',  args.reftime)
    CRTIME_COLS      = gen_time_cols('crtime', args.reftime)

    NAME_COLS        = gen_str_cols('name',        log2_name_len_bucket_count)
    LINKNAME_COLS    = gen_str_cols('linkname',    log2_name_len_bucket_count)
    XATTR_NAME_COLS  = gen_str_cols('xattr_name',  log2_name_len_bucket_count)
    XATTR_VALUE_COLS = gen_str_cols('xattr_value', log2_name_len_bucket_count)

    EXT_COLS = [
        # filenames without extensions pass in NULL
        ['hist', ['''category_hist(CASE WHEN name NOT LIKE '%.%' THEN
                                       {0}
                                   ELSE
                                       REPLACE(name, RTRIM(name, REPLACE(name, '.', '')), '')
                                   END)'''.format(SQLITE3_NULL),
                 SQLITE3_TEXT]]
    ]

    FIRST_PASS = [
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

    # generate columns for selecting and inserting into

    create_cols = [] # used for creating intermediate tables and aggregating
    select_cols = [] # SELECT cols FROM VRXPENTRIES

    # summary column names are not prefixed
    for col_name, stat in SUMMARY_COLS:
        sql, col_type = stat
        create_cols += ['{0} {1}'.format(col_name, col_type)]
        select_cols += [sql]

    for col_name, stats_pulled in FIRST_PASS:
        for stat_name, stat in stats_pulled:
            sql, col_type = stat
            create_cols += ['{0}_{1} {2}'.format(col_name, stat_name, col_type)]
            select_cols += [sql]

    # same for intermediate and aggregate
    table_cols_sql = ', '.join(create_cols)

    INTERMEDIATE = 'intermediate'
    # ###############################################################

    # ###############################################################
    # treesummary
    # copied from dbutils.h
    TREESUMMARY_CREATE = "CREATE TABLE {0}(totsubdirs INT64, maxsubdirfiles INT64, maxsubdirlinks INT64, maxsubdirsize INT64, totfiles INT64, totlinks INT64, minuid INT64, maxuid INT64, mingid INT64, maxgid INT64, minsize INT64, maxsize INT64, totzero INT64, totltk INT64, totmtk INT64, totltm INT64, totmtm INT64, totmtg INT64, totmtt INT64, totsize INT64, minctime INT64, maxctime INT64, minmtime INT64, maxmtime INT64, minatime INT64, maxatime INT64, minblocks INT64, maxblocks INT64, totxattr INT64, depth INT64, mincrtime INT64, maxcrtime INT64, minossint1 INT64, maxossint1 INT64, totossint1 INT64, minossint2 INT64, maxossint2 INT64, totossint2 INT64, minossint3 INT64, maxossint3 INT64, totossint3 INT64, minossint4 INT64, maxossint4 INT64, totossint4 INT64, rectype INT64, uid INT64, gid INT64);".format

    INTERMEDIATE_TREESUMMARY = 'intermediate_treesummary'
    # ###############################################################

    # construct full command to run
    cmd = [
        config.query(),
        config.indexroot(),
        '-n', str(config.threads()),
        '-x',
        '-O', args.outname,
        '-I', 'CREATE TABLE {0}({1}); {2};'.format(
            INTERMEDIATE, table_cols_sql, TREESUMMARY_CREATE(INTERMEDIATE_TREESUMMARY)),

        '-T', 'INSERT INTO {0} SELECT * FROM {1}; SELECT 1 FROM {1};'.format(
            INTERMEDIATE_TREESUMMARY, TREESUMMARY),

        '-E', 'INSERT INTO {0} {1};'.format(
            INTERMEDIATE, build_query(select_cols, [VRXPENTRIES])),

        '-K', 'CREATE TABLE {0}({1}); {2};'.format(
            SNAPSHOT, table_cols_sql, TREESUMMARY_CREATE(TREESUMMARY)),

        '-J', 'INSERT INTO {0} SELECT * FROM {1}; INSERT INTO {2} SELECT * FROM {3};'.format(
            SNAPSHOT, INTERMEDIATE, TREESUMMARY, INTERMEDIATE_TREESUMMARY),
    ]

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
                     (timestamp, config.indexroot(), args.notes))

        conn.commit()

    return 0

if __name__ == '__main__':
    run(sys.argv)
