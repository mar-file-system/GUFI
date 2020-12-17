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
import hashlib
import os
import subprocess
import sqlite3

import available
import Column

TABLE_NAME = 'configurations'

# set of columns that define a configuration
COLUMNS = [
    # machine information
    Column.Column('name',       'TEXT',
                  lambda parser, name: parser.add_argument('--name',
                                                           dest=name,
                                                           type=str, help='Machine name')),
    Column.Column('CPU'    ,    'TEXT',
                  lambda parser, name: parser.add_argument('--cpu',
                                                           dest=name,
                                                           type=str, help='CPU information')),
    Column.Column('memory',     'TEXT',
                  lambda parser, name: parser.add_argument('--memory',
                                                           dest=name,
                                                           type=str, help='Memory information')),
    Column.Column('storage',    'TEXT',
                  lambda parser, name: parser.add_argument('--storage',
                                                           dest=name,
                                                           type=str, help='Storage information')),
    Column.Column('extra',      'TEXT',
                  lambda parser, name: parser.add_argument('--extra',
                                                           dest=name,
                                                           type=str, help='Extra information')),
]

# Known hashes
HASHES = {
    'md5'    : hashlib.md5,
    'sha1'   : hashlib.sha1,
    'sha224' : hashlib.sha224,
    'sha256' : hashlib.sha256,
    'sha384' : hashlib.sha384,
    'sha512' : hashlib.sha512,
}

# use this SQL statement to initialize configuration tables will fail
# if table already exists
#
# the columns argument is the list of executable specific columns that
# will be appended to the ones listed above
def setup(cursor, columns):
    cursor.execute('''CREATE TABLE {} ({}, hash TEXT, UNIQUE({}));'''.format(
        TABLE_NAME,
        ', '.join([col.sql_column() for col in COLUMNS] +
                  [col.sql_column() for col in columns]),
        ', '.join([col.name         for col in COLUMNS] +
                  [col.name         for col in columns])
    ))

# convert some Python values to SQLite values
#     non-string -> str(value)
#     string     -> 'str'
#     None       -> 'None'
# config needs the attribute col.var
def python2sqlite(config, col):
    value = getattr(config, col.name)

    if (value == None) or (col.type == 'TEXT'):
        value = '"' + str(value) + '"'
    elif type(value) == bool:
        value = str(int(value))
    else:
        value = str(value)

    return value

# generate a hash using the configuration provided
def gen_hash(cols, config, hash_func=hashlib.sha512):
    return hash_func(
        ', '.join(
            [python2sqlite(config, col) for col in cols]
        )
    ).hexdigest()

# use this SQL statement to figure out whether or not this configuration already exists
def find(table_name, cols, hash_to_match):
    return '''SELECT {}, hash FROM {} WHERE hash LIKE '{}%';'''.format(
        ', '.join([col.name for col in cols]),
        table_name,
        hash_to_match
    )

# find all configuration hashes that match the given hash string
# zero and multiple matches will error
def get(cursor, columns, config_hash):
    cursor.execute(find(TABLE_NAME,
                        COLUMNS + columns,
                        config_hash))

    config = cursor.fetchall()

    if len(config) == 0:
        raise RuntimeError('''Could not find configuration hash {}.'''.format(config_hash))

    if len(config) > 1:
        raise RuntimeError('''Got {} rows. This should not happen.'''.format(len(config)))

    return config[0]

# run this script to generate the hash of a configuration
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GUFI Configuration Hash Generator')

    for col in COLUMNS:
        col.argparse(parser)

    parser.add_argument('--hash', choices=HASHES.keys(), default='sha512', help='Hash to use')
    parser.add_argument('--add',  metavar='db', type=str,                  help='Add this configuration entry to db')
    available.add_choices(parser, False)

    args, exec_specific = parser.parse_known_args()

    # parse executable specific arguments
    exec_parser = argparse.ArgumentParser(description='Executable specific argument parser')
    for config in args.executable.configuration:
        config.argparse(exec_parser)

    exec_args = exec_parser.parse_args(exec_specific)

    config_hash = gen_hash(COLUMNS, args, HASHES[args.hash])

    # add the config_hash to the database
    if args.add:
        if not os.path.isfile(args.add):
            raise RuntimeError('''Database file {} does not exist'''.format(args.add))

        db = sqlite3.connect(args.add)
        cursor = db.cursor()

        cursor.execute('''INSERT OR IGNORE INTO {} VALUES ({}, '{}');'''.format(
            TABLE_NAME,
            ', '.join([python2sqlite(args,      col) for col in COLUMNS] +
                      [python2sqlite(exec_args, col) for col in args.executable.configuration]),
            config_hash
        ))

        db.commit()
        db.close()

    print config_hash
