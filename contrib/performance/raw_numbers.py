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



import sqlite3

import Column

# These columns are used to uniquely identify statistics
# stored in a performance history database. They are
# prepended onto the user provided list of columns.
COLUMNS = [
    Column.Column('configuration', 'TEXT',
                  lambda parser, name: parser.add_argument('--config',
                                                           dest=name,
                                                           type=str, help='Configuration Hash'),
                  required=True),

    Column.Column('git', 'TEXT',
                  lambda parser, name: parser.add_argument('--git',
                                                           dest=name,
                                                           type=str, help='git commit hash'),
                  required=True),

    Column.Column('branch', 'TEXT',
                  lambda parser, name: parser.add_argument('--branch',
                                                           dest=name,
                                                           type=str, help='git branch name'),
                  required=True),

    Column.Column('run', 'INTEGER',
                  lambda parser, name: parser.add_argument('--run',
                                                           dest=name,
                                                           type=int, help='run number'),
                  required=True),
]

# Use this SQL statement to generate the SQL statemetn
# to create initialize the raw_numbers table.
# This statement will fail if table already exists.
#
# Each executable's create_table function should call this
# function to generate the raw_numbers table for that
# executable.
#
# The output to this function will be passed to the  setup
# function.
def create_table(table_name, columns):
    return '''CREATE TABLE {} ({}, UNIQUE({}), {}, {});'''.format(
        table_name,
        ', '.join([col.sql_column() for col in COLUMNS] +
                  [col.sql_column() for col in columns]),
        ', '.join([col.name for col in COLUMNS]),
        'FOREIGN KEY(configuration) REFERENCES configurations(hash) ON UPDATE CASCADE ON DELETE CASCADE',
        'FOREIGN KEY(git)           REFERENCES commits(hash)        ON UPDATE CASCADE ON DELETE CASCADE'
    )

# Use this function to create the tables for storing raw numbers.
#
# The create_table_sql_func should return a SQL statement that creates
# a table and optionally creates foreign keys that references
#
#     - the hash column of the 'configurations', which
#       should already exist
#
#     - the hash column of the 'commits' table, which
#       will be created right before create_table_sql_func
#       is called
#
# In order for the update/deletes to cascade, run
#
#     PRAGMA foreign_keys = ON;
#
# before performing an action because foreign keys
# are currently turned off by default.
def setup(cursor, table_name, columns):
    SQL = ['''CREATE TABLE commits(id INTEGER PRIMARY KEY, hash TEXT, UNIQUE(hash));''',
           create_table(table_name, columns)]

    for sql in SQL:
        cursor.execute(sql)
