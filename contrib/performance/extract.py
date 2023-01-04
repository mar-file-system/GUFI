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
import sqlite3
import sys

from performance_pkg import common
from performance_pkg.extraction import DebugPrints
from performance_pkg.hashdb import utils as hashdb

def get_current_commit():
    commit_hash = common.run_get_stdout(['@GIT_EXECUTABLE@', 'rev-parse', 'HEAD'])[:-1]

    # pylint: disable=consider-using-with
    diff = common.subprocess.Popen(['@GIT_EXECUTABLE@', 'diff-index', '--quiet', 'HEAD', '--'],
                                   stdout=common.DEVNULL, stderr=common.DEVNULL)
    diff.communicate() # no output
    if diff.returncode != 0:
        commit_hash += '~dirty'

    return commit_hash

def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument('--commit',
                        type=str,
                        help='Override current commit hash')
    parser.add_argument('--branch',
                        type=str,
                        help='Override current branch name')
    parser.add_argument('database',
                        type=str,
                        help='Hash database of configurations (must already exist)')
    parser.add_argument('raw_data_hash',
                        help='Hash of a single configuration')
    parser.add_argument('raw_data_db',
                        type=str,
                        help='Raw data database (must already exist)')

    return parser.parse_args(argv)

def run(argv):
    args = parse_args(argv)

    gufi_cmd, debug_name = hashdb.get_config(args.database, args.raw_data_hash)
    print('{0} was run with {1}, debug name {2}'.format(args.raw_data_hash, gufi_cmd, debug_name)) # pylint: disable=superfluous-parens

    # find functions for handling these debug prints
    debug_print = DebugPrints.DEBUG_PRINTS[gufi_cmd][debug_name]

    commit = args.commit
    if commit is None:
        commit = get_current_commit()

    branch = args.branch
    if branch is None:
        branch = common.run_get_stdout(['git', 'rev-parse', '--abbrev-ref', 'HEAD'])[:-1]

    # parse the output
    parsed = debug_print.extract(sys.stdin, commit, branch)

    # insert the parsed data into the raw data database
    hashdb.check_exists(args.raw_data_db)
    try:
        raw_data_db = sqlite3.connect(args.raw_data_db)
        debug_print.insert(raw_data_db, parsed)
        raw_data_db.commit()
    finally:
        raw_data_db.close()

if __name__ == '__main__':
    run(sys.argv[1:])
