#!/usr/bin/env python2.7
# This file is part of GUFI, which is part of MarFS, which is released
# under the BSD license.


# Copyright (c) 2017, Los Alamos National Security (LANS), LLC
# All rights reserved.

# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.

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

# -----
# NOTE:
# -----

# GUFI uses the C-Thread-Pool library.  The original version, written by
# Johan Hanssen Seferidis, is found at
# https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
# released under the MIT License.  LANS, LLC added functionality to the
# original work.  The original work, plus LANS, LLC added functionality is
# found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
# License.  The MIT License can be found at
# https://opensource.org/licenses/MIT.


# From Los Alamos National Security, LLC:
# LA-CC-15-039

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
import multiprocessing
import os
import pwd
import re
import subprocess
import sys

# ###############################################
# useful functions for using in ArgumentParser.add_argument(type=function_name)
# to make sure input fits certain criteria
# https://stackoverflow.com/a/14117511
def get_positive(value):
    '''Make sure the value is a positive integer.'''
    ivalue = int(value)
    if ivalue <= 0:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def get_non_negative(value):
    '''Make sure the value is a non-negative integer.'''
    ivalue = int(value)
    if ivalue < 0:
         raise argparse.ArgumentTypeError("%s is an invalid non-negative int value" % value)
    return ivalue

def get_char(value):
    '''Make sure the value is a single character.'''
    if len(value) != 1:
        raise argparse.ArgumentTypeError("%s is not a character" % value)
    return value

def get_size(value):
    '''Make sure the value matches [+/-]n[bcwkMG].'''
    if not re.match('^[+-]?[0-9]+[bcwkMG]?$', value):
        raise argparse.ArgumentTypeError("%s is not a valid size" % value)
    return value

def get_port(port):
    '''Make sure the value is an integer between 0 and 65536'''
    p = int(port)
    if (p < 0) or (65535 < p):
        raise argparse.ArgumentTypeError("Bad port: %d" % p)
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
    try:
        return pwd.getpwuid(int(uid_str)).pw_uid
    except:
        return uid

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
    except:
        return get_uid(user)

def get_gid(grp_str):
    '''
    Attempts to convert the input string to a gid.

    Args:
        group_str: A number stored as a string

    Returns:
        An int that is a gid
    '''

    gid = int(grp_str)
    try:
        return grp.getgruid(int(grp_str)).gr_gid
    except:
        return gid

def get_group(grp):
    '''
    Attempts to convert the input string to a gid.

    Args:
        group_str: A string gid or group name

    Returns:
        An int that is a gid
    '''

    try:
        return grp.getgrnam(grp).gr_gid
    except:
        return get_gid(grp)
# ###############################################

def build_query(select, tables, where = None, group_by = None, order_by = None, num_results = None, extra = None):
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

    if not select or not len(select) or not tables or not len(tables):
        return ""

    query = 'SELECT {} FROM {}'.format(', '.join(select), ', '.join(tables))

    if where and len(where):
        query += ' WHERE ({})'.format(') AND ('.join(where))

    if group_by and len(group_by):
        query += ' GROUP BY {}'.format(', '.join(group_by))

    if order_by and len(order_by):
        query += ' ORDER BY {}'.format(', '.join(order_by))

    if num_results:
        query += ' LIMIT {}'.format(num_results)

    if extra:
        query += ' ' + ' '.join(extra)

    return query + ';'

def cpus():
    '''Try to get the number of hardware threads'''
    try:
        return multiprocessing.cpu_count()
    except:
        return 1;
