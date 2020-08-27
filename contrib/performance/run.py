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
import os
import sqlite3
import subprocess

import available
import compare
import configuration
import raw_numbers
import stats

def git_hash(override_commit, ignore_dirty):
    if override_commit:
        return override_commit

    # get the git hash
    git_head = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE)
    version, _ = git_head.communicate()
    version = version.strip()

    if not ignore_dirty:
        # check if dirty
        git_dirty = subprocess.Popen(['git', 'status', '--porcelain', '--untracked-files=no'], stdout=subprocess.PIPE)
        dirty, _ = git_dirty.communicate()
        if len(dirty.strip()) != 0:
            version += '_dirty'

    return version

def git_branch():
    git_head = subprocess.Popen(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], stdout=subprocess.PIPE)
    branch, _ = git_head.communicate()
    return branch.strip()

def temp_name(name, stat):
    return '''temp.'{}_{}\''''.format(name, stat)

def insert_raw(cursor, commit, sql):
    # assume no previously existing rows with the same hash
    cursor.execute(sql)

    # add the hash into the commit table
    # temporary table not created for the hash before inserting
    cursor.execute('''INSERT OR IGNORE INTO commits VALUES (NULL, '{}');'''.format(commit))

# this should be run from within the repository
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Run a GUFI executable and store durations')

    parser.add_argument('--runs', metavar='n',
                        type=int, default=30,
                        help='How many times to run query')
    parser.add_argument('--add',
                        action='store_true',
                        help='Store results to database')
    parser.add_argument('--override-commit', metavar='hash',
                        type=str, default=None,
                        help='Use provided commit hash instead of hash obtained from git (implies --ignore-dirty)')
    parser.add_argument('--ignore-dirty',
                        action='store_true',
                        help='Do not suffix git commit with \'_dirty\'')
    parser.add_argument('database',
                        type=str,
                        help='database file name')
    parser.add_argument('config',
                        type=str,
                        help='configuration hash')

    # compare arguments
    parser.add_argument('--stat',
                        type=str, choices=[view.name for view in stats.VIEWS],
                        help='Statistics to compare after printing new values')
    parser.add_argument('--compare',
                        type=str, default=None,
                        help='Hash to compare this set of numbers against. Defaults to the last recorded commit.')
    available.add_choices(parser, True)
    compare.add_args(parser, True)

    args = parser.parse_args()

    if not os.path.isfile(args.database):
        raise IOError('''Database file {} does not exist. Not creating an empty database file.'''.format(args.database))

    selected = args.column[0]
    if not args.stat and len(selected):
        raise RuntimeError('''Need a statistic type to print''')

    perfdb = sqlite3.connect(args.database)
    stats.add_funcs(perfdb)
    cursor = perfdb.cursor()

    run_table_name = git_hash(args.override_commit, args.ignore_dirty)
    branch_name = git_branch()

    # create a temporary table for the timestamps
    cursor.execute('''CREATE TEMPORARY TABLE '{}' ({});'''.format(
        run_table_name,
        ', '.join([col.sql_column() for col in raw_numbers.COLUMNS] +
                  [col.sql_column() for col in args.executable.columns])
    ))

    # use the configuration to set up the runs
    query_cmd, config_hash, extra = args.executable.gen(cursor, args.config, args.executable_path)

    # run the query multiple times
    for run in xrange(args.runs):
        # run query, drop output, keep timings
        query = subprocess.Popen(query_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, lines = query.communicate()

        timings = args.executable.parse_output(lines, extra)

        # insert the new value into the temporary table
        cursor.execute('''INSERT INTO '{}' VALUES ('{}', '{}', '{}', {}, {});'''.format(
            run_table_name,
            config_hash,
            run_table_name,
            branch_name,
            run,
            ', '.join(timings)
        ))

    print 'Stats for {} runs of {} ({} branch) with configuration {}'.format(
        args.runs,
        run_table_name,
        branch_name,
        args.config
    )

    print 'Columns: ' + ', '.join(args.executable.column_names)

    # generate stats for this set of runs in separate temp views
    views = stats.VIEWS[:]
    for view in views:
        stat_name = view.name[:]
        view.name = temp_name(run_table_name, stat_name)

        cursor.execute(stats.create_view(run_table_name,
                                         args.executable.column_names,
                                         view,
                                         True))

        cursor.execute('''SELECT {} FROM {};'''.format(
            ', '.join(args.executable.column_names),
            view.name
        ))

        print '{}: {}'.format(stat_name, cursor.fetchall()[0])

    # compare new numbers with existing numbers
    if args.stat:
        previous = None
        if args.compare is None:
            cursor.execute('''SELECT hash FROM commits ORDER BY id DESC LIMIT 1;''')

            previous = cursor.fetchall()
            if len(previous) == 0:
                raise RuntimeError('''Could not find commit to compare against''')
        else:
            cursor.execute('''SELECT hash FROM commits WHERE hash LIKE '{}%';'''.format(args.compare))

            previous = cursor.fetchall()
            if len(previous) == 0:
                raise RuntimeError('''Could not match commit hash '{}'.'''.format(args.compare))
            elif len(previous) > 1:
                raise RuntimeError('''Found multiple matches for commit hash '{}'.'''.format(args.compare))

        compare.compare(cursor,
                        args.stat, selected,
                        previous[0][0], run_table_name,
                        args.stat, temp_name(run_table_name, args.stat))

    # move raw_numbers for this run into the raw_numbers table
    if args.add:
        insert_raw(cursor,
                   run_table_name,
                   '''INSERT INTO '{}' SELECT * FROM '{}';'''.format(args.executable.table_name, run_table_name)
        )

        perfdb.commit()

        print 'Saved {} runs of {} into {}'.format(args.runs, run_table_name, args.database)

    perfdb.close()
