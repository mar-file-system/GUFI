#!/usr/bin/env python3
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
import grp
import os
import pwd
import re
import sys

if (sys.version_info.major == 3) and (sys.version_info.minor < 3):
    from pipes import quote as sanitize # pylint: disable=deprecated-module
else:
    from shlex import quote as sanitize # new in Python 3.3

VERSION = '@VERSION_STRING@'

# how many 1024s to multiply together
SIZE_EXPONENTS = {
    'K':   1,
    'M':   2,
    'G':   3,
    'T':   4,
    'P':   5,
    'E':   6,
    # 'Z':   7,
    # 'Y':   8,
}

SIZES = [
#   1024  1000  1024
    'K',  'KB', 'KiB',
    'M',  'MB', 'MiB',
    'G',  'GB', 'GiB',
    'T',  'TB', 'TiB',
    'P',  'PB', 'PiB',
    'E',  'EB', 'EiB',
    # uint64_t can't go this high
    # 'Z',  'ZB', 'ZiB',
    # 'Y',  'YB', 'YiB',
]

FULL_TIME = 'full-iso'
LOCALE = 'locale'
TIME_STYLES = {
    FULL_TIME  : '%F %T %z',
    'long-iso' : '%F %T',
    'iso'      : '%m-%d %R',
    LOCALE     : '%b  %e %H:%M',
}

# sqlite3 types (not defined in standard sqlite3.py)
# https://www.sqlite.org/datatype3.html
# 2. Storage Classes and Datatypes
SQLITE3_NULL    = 'NULL'
SQLITE3_INTEGER = 'INTEGER'
SQLITE3_REAL    = 'REAL'
SQLITE3_TEXT    = 'TEXT'
SQLITE3_BLOB    = 'BLOB'

# 2.1. Boolean Datatype
SQLITE3_FALSE   = 'FALSE'
SQLITE3_TRUE    = 'TRUE'

# others common types that are recognized
SQLITE3_INT     = 'INT'
SQLITE3_INT64   = 'INT64'
SQLITE3_FLOAT   = 'FLOAT'
SQLITE3_DOUBLE  = 'DOUBLE'

# GUFI
# table names
ENTRIES     = 'entries'
SUMMARY     = 'summary'
PENTRIES    = 'pentries'
XSUMMARY    = 'xsummary'
XPENTRIES   = 'xpentries'
VRSUMMARY   = 'vrsummary'
VRPENTRIES  = 'vrpentries'
VRXSUMMARY  = 'vrxsummary'
VRXPENTRIES = 'vrxpentries'
TREESUMMARY = 'treesummary'

SUMMARY_NAMES = [
    SUMMARY,
    XSUMMARY,
    VRSUMMARY,
    VRXSUMMARY,
]

ENTRIES_NAMES = [
    ENTRIES,
    PENTRIES,
    XPENTRIES,
    VRPENTRIES,
    VRXPENTRIES,
]

# some common column names
LEVEL   = 'level'
INODE   = 'inode'
PINODE  = 'pinode'
PPINODE = 'ppinode'
UID     = 'uid'
GID     = 'gid'

# ###############################################
# useful functions for using in ArgumentParser.add_argument(type=function_name)
# to make sure input fits certain criteria
# https://stackoverflow.com/a/14117511
def get_positive(value):
    '''Make sure the value is a positive integer.'''
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("{0} is an invalid positive int value".format(value))
    return ivalue

def get_non_negative(value):
    '''Make sure the value is a non-negative integer.'''
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("{0} is an invalid non-negative int value".format(value))
    return ivalue

def get_char(value):
    '''Make sure the value is a single character.'''
    if len(value) != 1:
        raise argparse.ArgumentTypeError("{0} is not a character".format(value))
    return value

def get_size(value):
    '''Make sure the value matches [+/-]n[bcwkMG].'''
    if not re.match('^[+-]?[0-9]+[bcwkMG]?$', value):
        raise argparse.ArgumentTypeError("{0} is not a valid size".format(value))
    return value

def get_blocksize(value):
    '''Make sure the input is some combination of an optional integer and/or a valid optional unit.'''
    blocksize = re.match(r'^(\+?[0-9]*)({0})?$'.format('|'.join(SIZES)), value)
    if (blocksize is None) or ((blocksize.group(1) != '') and (int(blocksize.group(1)) == 0)):
        raise argparse.ArgumentTypeError('Invalid --block-size argument: \'{0}\''.format(value))

    return value

def get_port(port):
    '''Make sure the value is an integer between 0 and 65536'''

    # pylint: disable=invalid-name

    p = int(port)
    if (p < 0) or (p > 65535):
        raise argparse.ArgumentTypeError("Bad port: {0}".format(p))
    return p

def get_uid(uid_str):
    '''
    Attempts to convert a string into an integer

    Args:
        uid_str: An integer stored as a string

    Returns:
        An int that is an uid
    '''

    uid = int(uid_str)
    return pwd.getpwuid(uid).pw_uid

def get_user(user):
    '''
    Attempts to convert the input string into a uid

    Args:
        user: A string uid, or username

    Returns:
        An int that is an uid
    '''

    try:
        # use the string as a username
        return pwd.getpwnam(user).pw_uid
    except KeyError:
        return get_uid(user)

def get_gid(grp_str):
    '''
    Attempts to convert the input string to a gid.

    Args:
        grp_str: A number stored as a string

    Returns:
        An int that is a gid
    '''

    gid = int(grp_str)
    return grp.getgrgid(gid).gr_gid

def get_group(group):
    '''
    Attempts to convert the input string to a gid.

    Args:
        group: A string gid or group name

    Returns:
        An int that is a gid
    '''

    try:
        return grp.getgrnam(group).gr_gid
    except KeyError:
        return get_gid(group)
# ###############################################

# add this to summary queries to handle rollups
ROLLUP_SUMMARY_WHERE = '''
    CASE level()
        WHEN 0 THEN
            CASE rollupscore
                WHEN 0 THEN FALSE
                WHEN 1 THEN (isroot == 0 AND depth == 1)
            END
        WHEN 1 THEN
            (isroot == 1 AND depth == 0)
    END
'''

def build_query(select, tables, where=None, group_by=None,
                order_by=None, num_results=None, extra=None):
    '''
    Builds a query using arrays of data for each field.
    'select' and 'from_tables' must exist. All other arguments are optional.

    Args:
        select:      columns to SELECT from
        tables:      tables to query FROM
        where:       WHERE conditions
        group_by:    GROUP BY columns
        order_by:    ORDER BY columns
        num_results: number of results to limit by

    Returns:
        A query of the form:

            SELECT columns
            FROM table_names
            WHERE condtions     (optional)
            GROUP BY columns    (optional)
            ORDER BY columns    (optional)
            LIMIT num_results   (optional)
    '''

    # pylint: disable=too-many-arguments,too-many-positional-arguments

    if not select or len(select) == 0 or not tables or len(tables) == 0:
        return ''

    query = 'SELECT {0} FROM {1}'.format(', '.join(select), ', '.join(tables))

    if where and len(where):
        query += ' WHERE ({0})'.format(') AND ('.join(where))

    if group_by and len(group_by):
        query += ' GROUP BY {0}'.format(', '.join(group_by))

    if order_by and len(order_by):
        query += ' ORDER BY {0}'.format(', '.join(order_by))

    if num_results:
        query += ' LIMIT {0}'.format(num_results)

    if extra:
        # want leading space, or would have used str.join
        for sql in extra:
            query += ' {0}'.format(sql)

    return query

# a helper function to show the gufi_query being executed. Helpful for debugging and education.
# use escaping so it can be copy-pasted into a bash shell and executed correctly
def print_query(query_tokens):
    formatted_string = ''
    for index, token in enumerate(query_tokens):
        quoted_token = sanitize(token)
        if token.startswith('-') or index == len(query_tokens)-1:
            formatted_string += ' \\\n    ' + quoted_token
        else:
            formatted_string += ' ' + quoted_token
    print('GUFI query is \n  {0}'.format(formatted_string))
    sys.stdout.flush()

def add_delim_flag(parser):
    parser.add_argument('--delim',
                        dest='delim',
                        metavar='c',
                        type=get_char,
                        default=' ',
                        help='delimiter separating output columns')

def add_verbose_flag(parser):
    parser.add_argument('--verbose', '-V',
                        action='store_true',
                        help='Show the gufi_query being executed')


def add_common_flags(parser):
    '''Common GUFI tool flags'''
    add_delim_flag(parser)

    parser.add_argument('--in-memory-name',
                        dest='inmemory_name',
                        metavar='name',
                        type=str,
                        default='out',
                        help='Name of in-memory database when aggregation is performed')

    parser.add_argument('--aggregate-name',
                        dest='aggregate_name',
                        metavar='name',
                        type=str,
                        default='aggregate',
                        help='Name of final database when aggregation is performed')

    parser.add_argument('--skip-file',
                        dest='skip',
                        metavar='filename',
                        type=str,
                        default=None,
                        help='Name of file containing directory basenames to skip')

    add_verbose_flag(parser)

# check if a path is at or underneath the index root
def in_index(path, indexroot, orig, severity='Warning'):
    norm_path = os.path.normpath(path)
    term_path = os.path.join(norm_path, '')
    term_indexroot = os.path.join(indexroot, '')

    # make sure path 'abcd' does not match indexroot 'abc'
    # by comparing 'abcd/' and 'abc/'
    if term_path[:len(term_indexroot)] != term_indexroot:
        sys.stderr.write('{0}: Ignoring path "{1}", which is not under index root "{2}"\n'.format(severity, orig, indexroot))
        return False
    return True
