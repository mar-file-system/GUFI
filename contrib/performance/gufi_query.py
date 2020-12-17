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
    Column.Column('setup_globals', 'REAL',
                  lambda parser, name: parser.add_argument('--setup_globals',
                                                           dest=name,
                                                           type=float)),
    Column.Column('setup_aggregate', 'REAL',
                  lambda parser, name: parser.add_argument('--setup_aggregate',
                                                           dest=name,
                                                           type=float)),
    Column.Column('work', 'REAL',
                  lambda parser, name: parser.add_argument('--work',
                                                           dest=name,
                                                           type=float)),
    Column.Column('opendir', 'REAL',
                  lambda parser, name: parser.add_argument('--opendir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('opendb', 'REAL',
                  lambda parser, name: parser.add_argument('--opendb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sqlite3_open', 'REAL',
                  lambda parser, name: parser.add_argument('--sqlite3_open',
                                                           dest=name,
                                                           type=float)),
    Column.Column('set_pragmas', 'REAL',
                  lambda parser, name: parser.add_argument('--set_pragmas',
                                                           dest=name,
                                                           type=float)),
    Column.Column('load_extension', 'REAL',
                  lambda parser, name: parser.add_argument('--load_extension',
                                                           dest=name,
                                                           type=float)),
    Column.Column('addqueryfuncs', 'REAL',
                  lambda parser, name: parser.add_argument('--addqueryfuncs',
                                                           dest=name,
                                                           type=float)),
    Column.Column('descend', 'REAL',
                  lambda parser, name: parser.add_argument('--descend',
                                                           dest=name,
                                                           type=float)),
    Column.Column('check_args', 'REAL',
                  lambda parser, name: parser.add_argument('--check_args',
                                                           dest=name,
                                                           type=float)),
    Column.Column('level', 'REAL',
                  lambda parser, name: parser.add_argument('--level',
                                                           dest=name,
                                                           type=float)),
    Column.Column('level_branch', 'REAL',
                  lambda parser, name: parser.add_argument('--level_branch',
                                                           dest=name,
                                                           type=float)),
    Column.Column('while_branch', 'REAL',
                  lambda parser, name: parser.add_argument('--while_branch',
                                                           dest=name,
                                                           type=float)),
    Column.Column('readdir', 'REAL',
                  lambda parser, name: parser.add_argument('--readdir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('readdir_branch', 'REAL',
                  lambda parser, name: parser.add_argument('--readdir_branch',
                                                           dest=name,
                                                           type=float)),
    Column.Column('strncmp', 'REAL',
                  lambda parser, name: parser.add_argument('--strncmp',
                                                           dest=name,
                                                           type=float)),
    Column.Column('strncmp_branch', 'REAL',
                  lambda parser, name: parser.add_argument('--strncmp_branch',
                                                           dest=name,
                                                           type=float)),
    Column.Column('snprintf', 'REAL',
                  lambda parser, name: parser.add_argument('--snprintf',
                                                           dest=name,
                                                           type=float)),
    Column.Column('lstat', 'REAL',
                  lambda parser, name: parser.add_argument('--lstat',
                                                           dest=name,
                                                           type=float)),
    Column.Column('isdir', 'REAL',
                  lambda parser, name: parser.add_argument('--isdir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('isdir_branch', 'REAL',
                  lambda parser, name: parser.add_argument('--isdir_branch',
                                                           dest=name,
                                                           type=float)),
    Column.Column('access', 'REAL',
                  lambda parser, name: parser.add_argument('--access',
                                                           dest=name,
                                                           type=float)),
    Column.Column('set_struct', 'REAL',
                  lambda parser, name: parser.add_argument('--set_struct',
                                                           dest=name,
                                                           type=float)),
    Column.Column('clone', 'REAL',
                  lambda parser, name: parser.add_argument('--clone',
                                                           dest=name,
                                                           type=float)),
    Column.Column('pushdir', 'REAL',
                  lambda parser, name: parser.add_argument('--pushdir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('attach', 'REAL',
                  lambda parser, name: parser.add_argument('--attach',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sqltsumcheck', 'REAL',
                  lambda parser, name: parser.add_argument('--sqltsumcheck',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sqltsum', 'REAL',
                  lambda parser, name: parser.add_argument('--sqltsum',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sqlsum', 'REAL',
                  lambda parser, name: parser.add_argument('--sqlsum',
                                                           dest=name,
                                                           type=float)),
    Column.Column('sqlent', 'REAL',
                  lambda parser, name: parser.add_argument('--sqlent',
                                                           dest=name,
                                                           type=float)),
    Column.Column('detach', 'REAL',
                  lambda parser, name: parser.add_argument('--detach',
                                                           dest=name,
                                                           type=float)),
    Column.Column('closedb', 'REAL',
                  lambda parser, name: parser.add_argument('--closedb',
                                                           dest=name,
                                                           type=float)),
    Column.Column('closedir', 'REAL',
                  lambda parser, name: parser.add_argument('--closedir',
                                                           dest=name,
                                                           type=float)),
    Column.Column('utime', 'REAL',
                  lambda parser, name: parser.add_argument('--utime',
                                                           dest=name,
                                                           type=float)),
    Column.Column('free_work', 'REAL',
                  lambda parser, name: parser.add_argument('--free_work',
                                                           dest=name,
                                                           type=float)),
    Column.Column('output_timestamps', 'REAL',
                  lambda parser, name: parser.add_argument('--output_timestamps',
                                                           dest=name,
                                                           type=float)),
    Column.Column('aggregate', 'REAL',
                  lambda parser, name: parser.add_argument('--aggregate',
                                                           dest=name,
                                                           type=float)),
    Column.Column('output', 'REAL',
                  lambda parser, name: parser.add_argument('--output',
                                                           dest=name,
                                                           type=float)),
    Column.Column('cleanup_globals', 'REAL',
                  lambda parser, name: parser.add_argument('--cleanup_globals',
                                                           dest=name,
                                                           type=float)),
    Column.Column('rows', 'INTEGER',
                  lambda parser, name: parser.add_argument('--rows',
                                                           dest=name,
                                                           type=int)),
    Column.Column('query_count', 'INTEGER',
                  lambda parser, name: parser.add_argument('--query_count',
                                                           dest=name,
                                                           type=int)),
    Column.Column('RealTime', 'REAL',
                  lambda parser, name: parser.add_argument('--RealTime',
                                                           dest=name,
                                                           type=float)),
    Column.Column('ThreadTime', 'REAL',
                  lambda parser, name: parser.add_argument('--ThreadTime',
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
    Column.Column('aggregate_or_print', 'INTEGER',
                  lambda parser, name: parser.add_argument('--aggregate_or_print', '-e',
                                                           dest=name,
                                                           type=int)),
    Column.Column('init', 'TEXT',
                  lambda parser, name: parser.add_argument('--init', '-I',
                                                           dest=name,
                                                           type=str, help='-I query')),
    Column.Column('TreeSummary', 'TEXT',
                  lambda parser, name: parser.add_argument('--treesummary', '-T',
                                                           dest=name,
                                                           type=str, help='-T query')),
    Column.Column('Summary', 'TEXT',
                  lambda parser, name: parser.add_argument('--summary', '-S',
                                                           dest=name,
                                                           type=str, help='-S query')),
    Column.Column('Entries', 'TEXT',
                  lambda parser, name: parser.add_argument('--entries', '-E',
                                                           dest=name,
                                                           type=str, help='-E query')),
    Column.Column('Intermediate', 'TEXT',
                  lambda parser, name: parser.add_argument('--intermediate', '-J',
                                                           dest=name,
                                                           type=str, help='-J query')),
    Column.Column('Aggregate', 'TEXT',
                  lambda parser, name: parser.add_argument('--aggregate', '-G',
                                                           dest=name,
                                                           type=str, help='-G query')),
    Column.Column('Cleanup', 'TEXT',
                  lambda parser, name: parser.add_argument('--cleanup', '-F',
                                                           dest=name,
                                                           type=str, help='-F query')),
    Column.Column('AndOr', 'INTEGER',
                  lambda parser, name: parser.add_argument('--andor', '-a',
                                                           dest=name,
                                                           action='store_true')),
    Column.Column('utime', 'INTEGER',
                  lambda parser, name: parser.add_argument('--utime', '-m',
                                                           dest=name,
                                                           action='store_true')),
    Column.Column('MinLevel', 'INTEGER',
                  lambda parser, name: parser.add_argument('--minlevel', '-y',
                                                           dest=name,
                                                           type=int)),
    Column.Column('MaxLevel', 'INTEGER',
                  lambda parser, name: parser.add_argument('--maxlevel', '-z',
                                                           dest=name,
                                                           type=int)),
    Column.Column('OutputBufferSize', 'INTEGER',
                  lambda parser, name: parser.add_argument('--buffer_size',
                                                           dest=name,
                                                           type=int)),
    Column.Column('Terse', 'INTEGER',
                  lambda parser, name: parser.add_argument('--terse', '-j',
                                                           dest=name,
                                                           action='store_true')),
    Column.Column('GUFIIndex', 'TEXT',
                  lambda parser, name: parser.add_argument('GUFIIndex',
                                                           type=str),
                  required=True),
]

# find the configuration matching config_hash
# build the gufi_query command
# return the command, the full configuration hash, and whether the timestamps were terse
def get_numbers(cursor, config_hash, gufi_query):
    config = configuration.get(cursor, CONFIGURATION, config_hash)

    # parse configuration settings
    name, cpu, memory, storage, extra, threads, delimiter, aggregate_or_print, init, tsummary, summary, entries, intermediate, aggregate, cleanup, andor, utime, minlevel, maxlevel, buffersize, terse, index, full_hash = config

    # put together a gufi_query command with the configuration values
    query_cmd = [gufi_query]

    NONE = 'None'

    if terse != NONE:
        query_cmd += ['-j']

    if threads != NONE:
        query_cmd += ['-n', str(threads)]

    if delimiter != NONE:
        query_cmd += ['-d', delimiter]

    if aggregate_or_print != NONE:
        query_cmd += ['-e', str(aggregate_or_print)]

    if init != NONE:
        query_cmd += ['-I', init]

    if tsummary != NONE:
        query_cmd += ['-T', tsummary]

    if summary != NONE:
        query_cmd += ['-S', summary]

    if entries != NONE:
        query_cmd += ['-E', entries]

    if intermediate != NONE:
        query_cmd += ['-J', intermediate]

    if aggregate != NONE:
        query_cmd += ['-G', aggregate]

    if cleanup != NONE:
        query_cmd += ['-F', cleanup]

    if andor == '1':
        query_cmd += ['-a']

    if utime == '1':
        query_cmd += ['-m']

    if minlevel != NONE:
        query_cmd += ['-y', str(minlevel)]

    if maxlevel != NONE:
        query_cmd += ['-z', str(maxlevel)]

    if buffersize != NONE:
        query_cmd += ['-B', str(buffersize)]

    return query_cmd + [index], full_hash, (terse != NONE)


# split row into an array
def parse_terse(times):
    return times.strip().split()

# reformat human readable text into terse format
def parse_not_terse(output):
    lines = output.rsplit('\n', 46)[-46:46]

    terse = []
    for line in lines:
        if len(line):
            value = line.split()[-1]
            if value[-1] == 's':
                terse += [value[:-1]]
            else:
                terse += [value]

    return terse

def parse_output(lines, terse):
    if terse:
        return parse_terse(lines)
    return parse_not_terse(args, lines)
