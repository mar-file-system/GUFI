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



import Column
import configuration
import raw_numbers

TABLE_NAME = 'raw_numbers'

COLUMNS = [
    Column.Column('handle_args', 'REAL',
                  lambda parser, name: parser.add_argument('--handle_args',
                                                           dest=name,
                                                           type=float)),
    Column.Column('memset_work', 'REAL',
                  lambda parser, name: parser.add_argument('--memset_work',
                                                           dest=name,
                                                           type=float)),
    Column.Column('parse_dir', 'REAL',
                  lambda parser, name: parser.add_argument('--parse_dir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('dupdir', 'REAL',
                  lambda parser, name: parser.add_argument('--dupdir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('copy_template', 'REAL',
                  lambda parser, name: parser.add_argument('--copy_template',
                                                           dest=name,
                                                           type=float)),
    Column.Column('zero', 'REAL',
                  lambda parser, name: parser.add_argument('--zero',
                                                           dest=name,
                                                           type=float)),
    Column.Column('insertdbprep', 'REAL',
                  lambda parser, name: parser.add_argument('--insertdbprep',
                                                           dest=name,
                                                           type=float)),
    Column.Column('startdb', 'REAL',
                  lambda parser, name: parser.add_argument('--startdb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('fseek', 'REAL',
                  lambda parser, name: parser.add_argument('--fseek',
                                                           dest=name,
                                                           type=float)),
    Column.Column('read_entries', 'REAL',
                  lambda parser, name: parser.add_argument('--read_entries',
                                                           dest=name,
                                                           type=float)),
    Column.Column('getline', 'REAL',
                  lambda parser, name: parser.add_argument('--getline',
                                                           dest=name,
                                                           type=float)),
    Column.Column('memset_empty', 'REAL',
                  lambda parser, name: parser.add_argument('--memset_empty',
                                                           dest=name,
                                                           type=float)),
    Column.Column('parse_entry', 'REAL',
                  lambda parser, name: parser.add_argument('--parse_entry',
                                                           dest=name,
                                                           type=float)),
    Column.Column('free', 'REAL',
                  lambda parser, name: parser.add_argument('--free',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sumit', 'REAL',
                  lambda parser, name: parser.add_argument('--sumit',
                                                           dest=name,
                                                           type=float)),
    Column.Column('insertdbgo', 'REAL',
                  lambda parser, name: parser.add_argument('--insertdbgo',
                                                           dest=name,
                                                           type=float)),
    Column.Column('stopdb', 'REAL',
                  lambda parser, name: parser.add_argument('--stopdb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('insertdbfin', 'REAL',
                  lambda parser, name: parser.add_argument('--insertdbfin',
                                                           dest=name,
                                                           type=float)),
    Column.Column('insertsumdb', 'REAL',
                  lambda parser, name: parser.add_argument('--insertsumdb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('closedb', 'REAL',
                  lambda parser, name: parser.add_argument('--closedb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('cleanup', 'REAL',
                  lambda parser, name: parser.add_argument('--cleanup',
                                                           dest=name,
                                                           type=float)),
    Column.Column('directories', 'INTEGER',
                  lambda parser, name: parser.add_argument('--directories',
                                                           dest=name,
                                                           type=int)),
    Column.Column('files', 'INTEGER',
                  lambda parser, name: parser.add_argument('--files',
                                                           dest=name,
                                                           type=int)),
    Column.Column('main', 'REAL',
                  lambda parser, name: parser.add_argument('--main',
                                                           dest=name,
                                                           type=float)),
]

CONFIGURATION = [
    Column.Column('threads', 'INTEGER',
                  lambda parser, name: parser.add_argument('--threads', '-n',
                                                           dest=name,
                                                           type=int)),
    Column.Column('delimiter', 'TEXT',
                  lambda parser, name: parser.add_argument('--delimiter', '-d',
                                                           dest=name,
                                                           type=str)),
    Column.Column('input_file', 'TEXT',
                  lambda parser, name: parser.add_argument('input_file',
                                                           type=str),
                  required=True),
    Column.Column('output_dir', 'TEXT',
                  lambda parser, name: parser.add_argument('output_dir',
                                                           type=str),
                  required=True),
]

# find the configuration matching config_hash
# build the gufi_trace2index command
# return the command, the full configuration hash, and whether the timestamps were terse
def get_numbers(cursor, config_hash, gufi_query):
    config = configuration.get(cursor, CONFIGURATION, config_hash)

    # parse configuration settings
    name, cpu, memory, storage, extra, threads, delimiter, input_file, output_dir, full_hash = config

    # put together a gufi_query command with the configuration values
    query_cmd = [gufi_query]

    NONE = 'None'

    if threads != NONE:
        query_cmd += ['-n', str(threads)]

    if delimiter != NONE:
        query_cmd += ['-d', delimiter]

    return query_cmd + [input_file, output_dir], full_hash, False

def parse_output(lines, terse):
    lines = lines.rsplit('\n', 26)[-26:26]

    raw = []
    for line in lines[:-1]:
        if len(line):
            value = line.split()[-1]
            if value[-1] == 's':
                raw += [value[:-1]]
            else:
                raw += [value]

    return raw + [lines[-1].split()[-2]]
