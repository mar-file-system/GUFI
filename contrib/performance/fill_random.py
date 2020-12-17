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
import hashlib
import random
import sqlite3

import available
import configuration
import run

BRANCH_NAME = 'random_data'

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Fill a raw numbers table will random data')

    parser.add_argument('--commits',     type=int, default=10,                help='How many sets of data to generate')
    parser.add_argument('--runs',        type=int, default=30,                help='How many runs are used to generate a set of statistics')
    parser.add_argument('--int-range',   type=int, default=[0, 100], nargs=2, help='random integer range')
    parser.add_argument('--float-range', type=int, default=[0, 100], nargs=2, help='random float range')
    parser.add_argument('database',      type=str,                            help='database file name')
    parser.add_argument('config',        type=str,                            help='configuration hash')

    available.add_choices(parser, False)

    args = parser.parse_args()

    db = sqlite3.connect(args.database)
    cursor = db.cursor()

    # will raise if got bad config hash
    config_hash = configuration.get(cursor, args.executable.configuration, args.config)[-1]

    # seed commit
    fake_commit = hashlib.sha1(str(hash(random.uniform(0, 100))))

    # fill in database
    for _ in xrange(args.commits):
        commit = fake_commit.hexdigest()

        for r in xrange(args.runs):
            # generate random numbers
            # probably want to generate outside of loop
            # and generate changes between runs and commits
            times = []
            for col in args.executable.columns:
                value = random.gauss(50, 10)
                if col.type == 'INTEGER':
                    value = int(value)
                times += [str(value)]

            # insert into raw_numbers
            run.insert_raw(cursor,
                           commit,
                           '''INSERT INTO {} VALUES ('{}', '{}', '{}', {}, {});'''.format(
                               args.executable.table_name,
                               config_hash,
                               commit,
                               BRANCH_NAME,
                               r,
                               ', '.join(times)))

        # rehash previous commit to get next commit
        fake_commit = hashlib.sha1(fake_commit.digest())

    db.commit()
    db.close()
