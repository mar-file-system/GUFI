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



import unittest

# the binaries in contrib/performance
import gufi_hash
import machine_hash
import raw_data_hash

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
                      '--storage', '']
        expected_hash = '58b63f5433e2fa2f054c132e363beaaa'

        self.template(machine_hash.parse_args, input_args,
                      machine_hash.compute_hash, expected_hash)

    def test_gufi_hash(self):
        input_args = ['--hash_alg', 'md5',
                      '-n', '1',
                      '-S', '',
                      '-E', '',
                      'gufi_query',
                      'cumulative-times',
                      'tree']
        expected_hash = 'b21cc57f0628f8a76c0068c73933c140'

        self.template(gufi_hash.parse_args, input_args,
                      gufi_hash.compute_hash, expected_hash)

    def test_raw_data_hash(self):
        input_args = ['--hash_alg', 'md5',
                      'machine_hash',
                      'gufi_hash']
        expected_hash = '7d0d8a99bf419f1f5d68ddc026edb517'

        self.template(raw_data_hash.parse_args, input_args,
                      raw_data_hash.compute_hash, expected_hash)

if __name__ == '__main__':
    unittest.main()
