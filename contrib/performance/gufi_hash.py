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
import os
import sqlite3
import sys

import gufi_common

from performance_pkg.hashdb import gufi, utils as hashdb

def format_for_hash(attr, value, type): # pylint: disable=redefined-builtin
    # pylint: disable=no-else-return
    if type == str:
        return '-{0} "{1}"'.format(attr, value)
    elif type == int:
        return '-{0} {1}'.format(attr, value)
    elif type == bool:
        if value:
            return '-{0}'.format(attr)
        else:
            return None
    return str(value)

def compute_hash(args):
    if args.override:
        return args.override

    formatted = [
        args.hash_alg,
        args.cmd,
    ]
    for col, _, type in gufi.COLS_HASHED: # pylint: disable=redefined-builtin
        val = getattr(args, col)
        if val is not None:
            formatted_val = format_for_hash(col, val, type)
            # check for unset boolean flags
            if formatted_val is not None:
                formatted += [formatted_val]
    formatted += [args.tree]

    return hashdb.hash_config(args.hash_alg, ' '.join(formatted))

def parse_args(argv):
    parser = argparse.ArgumentParser()
    # parser.add_argument('-H',
    #                     action= 'store_true',
    #                     help= 'show assigned input values')
    parser.add_argument('-x',
                        action='store_true',
                        help='enable xattr processing')
    # parser.add_argument('-p',
    #                     action='store_true',
    #                     help='print file-names')
    # parser.add_argument('-P',
    #                     action='store_true',
    #                     help='print directories as they are encountered')
    # parser.add_argument('-N',
    #                     action='store_true',
    #                     help= 'print column-names (header) for DB results')
    # parser.add_argument('-V',
    #                     action='store_true',
    #                     help= 'print column-values (rows) for DB results')
    # parser.add_argument('-s',
    #                     action='store_true',
    #                     help='generate tree-summary table (in top-level DB)')
    # parser.add_argument('-b',
    #                     action='store_true',
    #                     help='build GUFI index tree')
    parser.add_argument('-a',
                        action='store_true',
                        help='AND/OR (SQL query combination)')
    parser.add_argument('-n',
                        metavar='<threads>',
                        type=gufi_common.get_non_negative,
                        default=1,
                        help='number of threads')
    parser.add_argument('-d',
                        metavar='<delim>',
                        type=str,
                        default='\x1e',
                        help='delimiter (one char)  [use \'x\' for \\x1E]')
    # parser.add_argument('-i',
    #                     metavar='<input_dir>',
    #                     type=str
    #                     help='input directory path')
    # parser.add_argument('-t',
    #                     metavar='<to_dir>',
    #                     help='build GUFI index (under) here')
    parser.add_argument('-o',
                        metavar='<out_fname>',
                        type=str,
                        help='output file (one-per-thread, with thread-id suffix)')  # noqa
    parser.add_argument('-O',
                        metavar='<out_DB>',
                        type=str,
                        help='output DB')
    parser.add_argument('-I',
                        metavar='<SQL_init>',
                        type=str,
                        help='SQL init')
    parser.add_argument('-T',
                        metavar='<SQL_tsum>',
                        type=str,
                        help='SQL for tree-summary table')
    parser.add_argument('-S',
                        metavar='<SQL_sum>',
                        type=str,
                        help='SQL for summary table')
    parser.add_argument('-E',
                        metavar='<SQL_ent>',
                        type=str,
                        help='SQL for entries table')
    parser.add_argument('-F',
                        metavar='<SQL_fin>',
                        type=str,
                        help='SQL cleanup')
    # parser.add_argument('-r',
    #                     action='store_true',
    #                     help='insert files and links into db (for bfwreaddirplus2db)')
    # parser.add_argument('-R',
    #                     action='store_true',
    #                     help='insert dires into db (for bfwreaddirplus2db'))
    # parser.add_argument('-D',
    #                     action='store_true',
    #                     help='don\'t descend the tree')
    # parser.add_argument('-Y',
    #                     action='store_true',
    #                     help='default to all directories suspect')
    # parser.add_argument('-Z',
    #                     action='store_true',
    #                     help='default to all files/links suspect')')
    # parser.add_argument('-W',
    #                     metavar='<INSUSPECT>',
    #                     type=str,
    #                     help='suspect input file')
    # parser.add_argument('-A',
    #                     metavar='<suspectmethod>',
    #                     choices=[1,2,3],
                        # help='suspect method (0 no suspects, '
                        #      '1 suspect file_dbl, 2 suspect '
                        #      'stat d and file_fl, 3 suspect stat_dbl')
    # parser.add_argument('-g',
    #                     metavar='<stridesize>',
    #                     type=int,
    #                     help= 'stride size for striping inodes')
    # parser.add_argument('-c',
    #                     metavar='<suspecttime>',
    #                     type=int,
    #                     help='number of threads')
    # parser.add_argument('-u',
    #                     action='store_true',
    #                     help='input mode is from a file so input is a file not a dir')
    parser.add_argument('-y',
                        metavar='<min_level>',
                        type=int,
                        help='minimum level to go down')
    parser.add_argument('-z',
                        metavar='<max_level>',
                        type=int,
                        help='maximum level to go down')
    parser.add_argument('-J',
                        metavar='<SQL_interm>',
                        type=str,
                        help='SQL for intermediate results')
    parser.add_argument('-K',
                        metavar='<create aggregate>',
                        type=str,
                        help='SQL to create the final aggregation table')
    parser.add_argument('-G',
                        metavar='<SQL_aggregate>',
                        type=str,
                        help='SQL for aggregated results')
    parser.add_argument('-m',
                        action='store_true',
                        help='Keep mtime and atime same on the database files')
    parser.add_argument('-B',
                        metavar='<buffer size>',
                        type=gufi_common.get_non_negative,
                        help='size of each thread\'s output buffer in bytes')
    parser.add_argument('-w',
                        action='store_true',
                        help='open the database files in read-write mode instead of read only mode')

    # Non gufi command_flags
    parser.add_argument('--hash_alg',
                        default='md5',
                        choices=hashdb.Hashes.keys(),
                        help='Hash algorithm')
    parser.add_argument('--override',
                        type=str,
                        help='Use this value for the hash instead of the calculated value')
    parser.add_argument('--extra',
                        type=str,
                        help='Additional notes (not hashed)')
    parser.add_argument('--database',
                        type=str,
                        help='Hash database to write to (must already exist)')
    parser.add_argument('--delete',
                        action='store_true',
                        help='Remove record from database')

    parser.add_argument('cmd',
                        choices=gufi.COMMANDS,
                        help='GUFI command')
    parser.add_argument('debug_name',
                        choices=gufi.DEBUG_NAME,
                        help='GUFI debug name')
    parser.add_argument('tree',
                        type=str,
                        help='Tree that was processed')

    return parser.parse_args(argv)

def run(argv):
    args = parse_args(argv)

    # this should be done during parse_args, but
    # doing here instead to make testing easier
    args.tree = os.path.realpath(args.tree)

    # hash(gufi command, benchmark name, flags, tree)
    gufi_hash = compute_hash(args)
    print(gufi_hash) # pylint: disable=superfluous-parens

    if args.database:
        try:
            con = sqlite3.connect(args.database)

            if args.delete:
                con.execute('DELETE FROM {0} WHERE hash == "{1}";'.format(
                    gufi.TABLE_NAME, gufi_hash))
            else:
                hashdb.insert(con, args, gufi_hash,
                              gufi.TABLE_NAME,
                              gufi.COLS_REQUIRED,
                              gufi.COLS_HASHED,
                              gufi.COLS_NOT_HASHED)

            con.commit()
        finally:
            con.close()

if __name__ == '__main__':
    run(sys.argv[1:])
