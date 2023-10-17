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
import grp
import pwd
import re
import sys

# table names
SUMMARY     = 'summary'
PENTRIES    = 'pentries'
XSUMMARY    = 'xsummary'
XPENTRIES   = 'xpentries'
VRSUMMARY   = 'vrsummary'
VRPENTRIES  = 'vrpentries'
VRXSUMMARY  = 'vrxsummary'
VRXPENTRIES = 'vrxpentries'

SUMMARY_NAMES = [
    SUMMARY,
    XSUMMARY,
    VRSUMMARY,
    VRXSUMMARY,
]

ENTRIES_NAMES = [
    'entries',
    PENTRIES,
    XPENTRIES,
    VRPENTRIES,
    VRXPENTRIES,
]

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

    # pylint: disable=too-many-arguments

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
def print_query(query_tokens):
    formatted_string = ""
    for index, token in enumerate(query_tokens):
        if token.startswith('-') or index == len(query_tokens)-1:
            formatted_string += '\n    ' + token
        else:
            formatted_string += ' ' + token
    print('GUFI query is \n  {0}'.format(formatted_string))
    sys.stdout.flush()

def add_common_flags(parser):
    '''Common GUFI tool flags'''
    parser.add_argument('--delim',
                        dest='delim',
                        metavar='c',
                        type=get_char,
                        default=' ',
                        help='delimiter separating output columns')

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

    parser.add_argument('--verbose', '-V',
                        action='store_true',
                        help='Show the gufi_query being executed')
