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



import os
import sqlite3
import tempfile
import unittest

# the executables in contrib/performance
import gufi_hash
import machine_hash
import raw_data_hash
import setup_hashdb
import setup_raw_data_db

OVERRIDE = 'override'

class TestHashing(unittest.TestCase):
    def template(self, parse_args, argv,
                 compute_hash, expected_hash):
        args = parse_args(argv)
        self.assertEqual(expected_hash, compute_hash(args))

        # extra notes don't affect the hash
        args = parse_args(argv + ['--extra', ''])
        self.assertEqual(expected_hash, compute_hash(args))

        # input changes change the hash
        modified_args = argv[:]
        modified_args[-1] += ' '
        args = parse_args(modified_args)
        modified_hash = compute_hash(args)
        self.assertNotEqual(expected_hash, modified_hash)

        # extra notes don't affect the hash
        args = parse_args(modified_args + ['--extra', ''])
        self.assertEqual(modified_hash, compute_hash(args))

        # hash override
        args = parse_args(argv + ['--override', OVERRIDE])
        self.assertEqual(OVERRIDE, compute_hash(args))

    def test_machine_hash(self):
        input_args = ['--hash_alg', 'md5',
                      '--name', '',
                      '--cpu', '',
                      '--cores', '0',
                      '--ram', '0',
                      '--storage', '',
                      '--os', '',
                      '--kernel', '']
        expected_hash = 'b039dc6a0a31d48453c01be909e504c5'

        self.template(machine_hash.parse_args, input_args,
                      machine_hash.compute_hash, expected_hash)

    def test_gufi_hash(self):
        input_args = ['--hash_alg', 'md5',
                      '-n', '1',
                      '-S', '',
                      '-E', '',
                      'gufi_query',
                      'cumulative_times',
                      'tree']
        expected_hash = '507417ff9005cf3d2561ec641fb81aa8'

        self.template(gufi_hash.parse_args, input_args,
                      gufi_hash.compute_hash, expected_hash)

    def test_raw_data_hash(self):
        input_args = ['--hash_alg', 'md5',
                      'machine_hash',
                      'gufi_hash']
        expected_hash = '7d0d8a99bf419f1f5d68ddc026edb517'

        self.template(raw_data_hash.parse_args, input_args,
                      raw_data_hash.compute_hash, expected_hash)

class IntegrationTest(unittest.TestCase): # pylint: disable=too-many-instance-attributes
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.hashes_name = os.path.abspath(os.path.join(self.tempdir, 'hashes.db'))
        self.machine_hash = 'machine_hash'
        self.gufi_cmd = 'gufi_query'
        self.debug_name = 'cumulative_times'
        self.gufi_hash = 'gufi_hash'
        self.raw_data_hash = 'raw_data_hash'

    def opendb(self):
        # file:name?mode=ro doesn't work in Python2
        setup_hashdb.hashdb.check_exists(self.hashes_name)
        return sqlite3.connect(self.hashes_name)

    def check_table(self, table_name, expected_hash, test_name):
        try:
            db = self.opendb()

            cur = db.execute('SELECT hash FROM {0};'.format(table_name))
            res = cur.fetchall()
            if expected_hash is None:
                self.assertEqual(len(res), 0)
            else:
                self.assertEqual(len(res), 1)
                self.assertEqual(res[0][0], expected_hash)
        except Exception as err: # pylint: disable=broad-except
            self.fail('Testing {0} raised: {1}'.format(test_name, err))
        finally:
            db.close()

    # see the Setup section in the README for descriptions

    # step 1 is setting up PYTHONPATH

    def step2(self):
        setup_hashdb.run([self.hashes_name])

        db = self.opendb()
        try:
            # make sure tables exist
            cur = db.execute('SELECT name FROM sqlite_master WHERE type == "table";')
            res = cur.fetchall()
            self.assertEqual(len(res), 3)
            for table_name in res:
                self.assertIn(table_name[0], [machine_hash.machine.TABLE_NAME,
                                              gufi_hash.gufi.TABLE_NAME,
                                              raw_data_hash.raw_data.TABLE_NAME])
        except Exception as err: # pylint: disable=broad-except
            self.fail('Testing setup_hashdb raised: {0}'.format(err))
        finally:
            db.close()

    def step3(self):
        machine_hash.run(['--override', self.machine_hash,
                          '--database', self.hashes_name])
        self.check_table(machine_hash.machine.TABLE_NAME, self.machine_hash, 'machine_hash')

    def step4(self):
        gufi_hash.run([self.gufi_cmd,
                       self.debug_name,
                       'tree',
                       '--override', self.gufi_hash,
                       '--database', self.hashes_name])

        self.check_table(gufi_hash.gufi.TABLE_NAME, self.gufi_hash, 'gufi_hash')

    def step5(self):
        raw_data_hash.run([self.machine_hash,
                           self.gufi_hash,
                           '--override', self.raw_data_hash,
                           '--database', self.hashes_name])

        self.check_table(raw_data_hash.raw_data.TABLE_NAME, self.raw_data_hash, 'raw_data_hash')

    def step6(self):
        raw_data_db_name = os.path.abspath(os.path.join(self.tempdir, 'raw_data.db'))
        setup_raw_data_db.run([self.hashes_name, self.raw_data_hash, raw_data_db_name])

        try:
            setup_hashdb.hashdb.check_exists(raw_data_db_name)
        except SystemExit:
            self.fail('setup_raw_data_db did not create a database file')

        db = sqlite3.connect(raw_data_db_name)
        try:
            cur = db.execute('SELECT name FROM sqlite_master WHERE type == "table";')
            res = cur.fetchall()
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0][0], self.debug_name)
        except Exception as err: # pylint: disable=broad-except
            self.fail('Testing setup_raw_data_db raised: {0}'.format(err))
        finally:
            db.close()
            os.remove(raw_data_db_name)

    # raw_data_hash --delete
    def delete5(self):
        raw_data_hash.run([self.machine_hash,
                           self.gufi_hash,
                           '--delete',
                           '--override', self.raw_data_hash,
                           '--database', self.hashes_name])

        self.check_table(raw_data_hash.raw_data.TABLE_NAME, None, 'raw_data_hash')

    # gufi_hash --delete
    def delete4(self):
        gufi_hash.run([self.gufi_cmd,
                       self.debug_name,
                       'tree',
                       '--delete',
                       '--override', self.gufi_hash,
                       '--database', self.hashes_name])

        self.check_table(gufi_hash.gufi.TABLE_NAME, None, 'gufi_hash')

    # machine_hash --delete
    def delete3(self):
        machine_hash.run(['--delete',
                          '--override', self.machine_hash,
                          '--database', self.hashes_name])

        self.check_table(machine_hash.machine.TABLE_NAME, None, 'machine_hash')

    def test_setup(self):
        self.step2()
        self.step3()
        self.step4()
        self.step5()
        self.step6()

        # test --delete
        self.delete5()
        self.delete4()
        self.delete3()

    def tearDown(self):
        os.remove(self.hashes_name)
        os.rmdir(self.tempdir)

if __name__ == '__main__':
    unittest.main()
