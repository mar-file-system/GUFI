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



import re

from performance_pkg import common

# common functions used to process multiple debug print formats
# these functions ignore column order
# "columns" arguments should be dictionaries

def create_table(con, table_name, columns):
    # all column names need to be surrounded by quotation marks, even ones that don't have spaces
    cols = ', '.join('"{0}" {1}'.format(col, common.TYPE_TO_SQLITE[type]) for col, type in columns.items())
    con.execute('CREATE TABLE {0} ({1});'.format(table_name, cols))

# <required>, [optional]
# [whitespace]<column name>[:] <number>[s][whitespace]
COLUMN_PATTERN = re.compile(r'^\s*(.+?):? +(\d+(\.\d*)?)s?\s*$')

def cumulative_times_extract(src, commit, branch, columns, known_formats):
    data = {}

    # parse input
    for line in src:
        line = line.strip()
        if line == '':
            continue

        match = COLUMN_PATTERN.match(line)

        # failed to match
        if match is None:
            continue

        col_name = match.group(1)

        # line has correct format, but column
        # name is not known, so skip
        if col_name not in columns:
            continue

        # store valid data
        data[col_name] = match.group(2)

    # make sure parsed data matches one known format
    column_format_match = False
    for known_format in known_formats:
        if len(data) != len(known_format):
            continue

        column_format_match = True
        for col in known_format:
            if col not in data:
                column_format_match = False
                break

        if column_format_match:
            break

    if not column_format_match:
        raise ValueError('Cumulative times data matches no known format on commit {0}'.format(commit))

    # these aren't obtained from running gufi_query
    data.update({
        'id'    : None,
        'commit': commit,
        'branch': branch,
    })

    return data

# helper function
def format_value(value, type): # pylint: disable=redefined-builtin
    # pylint: disable=no-else-return
    if type is None:
        return 'NULL'
    elif type == str:
        return '"{0}"'.format(value)
    return str(value)

def insert(con, parsed, table_name, columns):
    # parsed should have data for ALL columns of a particular format,
    # which is a subset of the union of all column formats
    columns_format = [[col, type] for col, type, in columns.items() if col in parsed]

    # create SQL statement for inserting with only the found columns
    cols = ', '.join('"{0}"'.format(col) for col, _ in columns_format)
    vals = ', '.join(format_value(parsed[col], type) for col, type in columns_format)
    con.execute('INSERT INTO {0} ({1}) VALUES ({2});'.format(table_name, cols, vals))
