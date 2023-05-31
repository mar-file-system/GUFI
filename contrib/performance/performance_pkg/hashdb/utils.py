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



import os
import sys

from hashes import Hashes # not part of performance_pkg

from performance_pkg import common
from performance_pkg.hashdb import commits, machine, gufi, raw_data

def check_exists(path):
    if not os.path.exists(path):
        print("'{0}' does not exist!".format(path)) # pylint: disable=superfluous-parens
        sys.exit(1)

    if not os.path.isfile(path):
        print("'{0}' is not a file!".format(path)) # pylint: disable=superfluous-parens
        sys.exit(2)

def check_not_exists(path):
    if os.path.exists(path):
        print("'{0}' already exists!".format(path)) # pylint: disable=superfluous-parens
        sys.exit(1)

# set up tables
def create_tables(con):
    # first column for machine, gufi and raw_data tables
    hash_col = [['hash', None, str]]

    machine_col_str = ', '.join('{0} {1}'.format(
        name if name else col, common.TYPE_TO_SQLITE[type]) for col, name, type in hash_col + machine.COLS)
    gufi_col_str = ', '.join('{0} {1}'.format(
        name if name else col, common.TYPE_TO_SQLITE[type]) for col, name, type in hash_col + gufi.COLS)
    raw_data_col_str = ', '.join('{0} {1}'.format(
        name if name else col, common.TYPE_TO_SQLITE[type]) for col, name, type in hash_col + raw_data.COLS)

    # create machine, gufi, and raw_data tables
    con.execute('CREATE TABLE {0} ({1}, PRIMARY KEY (hash));'.format(
        machine.TABLE_NAME, machine_col_str))
    con.execute('CREATE TABLE {0} ({1}, PRIMARY KEY (hash));'.format(
        gufi.TABLE_NAME, gufi_col_str))
    con.execute('CREATE TABLE {0} ({1}, PRIMARY KEY (hash));'.format(
        raw_data.TABLE_NAME, raw_data_col_str))

    # the commits table gets created and filled
    commits_col_str = ', '.join('"{0}" {1}'.format(
        name if name else col, common.TYPE_TO_SQLITE[type]) for col, name, type in commits.COLS)

    con.execute('CREATE TABLE {0} ({1});'.format(
        commits.TABLE_NAME, commits_col_str))
    commits.fill_table(con)

# hash a configuration, not configure the hash algorithm
def hash_config(alg, data):
    return Hashes[alg](data.encode()).hexdigest()

def insert(con, args, hash, table_name, cols_hashed, cols_not_hashed):
    '''
    only insert args/columns that are present rather than insert
    filler values into the database - let sqlite set the defaults

    args should contains members with the same names as the columns
    '''

    # pylint: disable=redefined-builtin,too-many-arguments

    cols = ['hash']
    vals = ['"{0}"'.format(hash)]

    for col, name, type in cols_hashed + cols_not_hashed:
        val = getattr(args, col)
        if val is not None:
            cols += [name if name else col]

            if type == str:
                vals += ['"{0}"'.format(val)]
            elif type == bool:
                vals += [str(int(val))]
            else:
                vals += [str(val)]

    # leave as separate variable for debug
    sql = 'INSERT INTO {0} ({1}) VALUES ({2});'.format(
        table_name, ', '.join(cols), ', '.join(vals))

    con.execute(sql)

# extract gufi command and debug name using provided hash
def get_config(con, user_raw_data_hash):
    # figure out what configurations this hash points to
    hashes_cur = con.execute('SELECT hash, machine_hash, gufi_hash ' +
                             'FROM {0} WHERE hash GLOB "*{1}*";'.format(
                                 raw_data.TABLE_NAME, user_raw_data_hash))
    records = hashes_cur.fetchall()
    count = len(records)

    if count == 0:
        raise KeyError('"{0}" did not match any known hashes'.format(user_raw_data_hash))

    if count > 1:     # can happen if partial hash is provided
        found = '\n'.join(['    {0} -> {1} {2}'.format(raw_data_hash, machine_hash, gufi_hash)
                           for raw_data_hash, machine_hash, gufi_hash in records])
        raise ValueError('"{0}" matched {1} hashes:\n{2}'.format(user_raw_data_hash, count, found))

    # guaranteed only 1 record can be found at this point
    _, _, gufi_hash = records[0]

    # figure out which gufi command and set of debug numbers this hash is associated with
    info_cur = con.execute('SELECT {0}, {1} FROM {2};'.format(
        gufi.COL_CMD, gufi.COL_DEBUG_NAME, gufi.TABLE_NAME))
    gufi_cmd, debug_name = info_cur.fetchall()[0]

    return gufi_cmd, debug_name
