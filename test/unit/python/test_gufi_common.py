#!@Python3_EXECUTABLE@
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
import sys
import unittest

sys.path += [
    # scripts/gufi_common.py
    os.path.join('@CMAKE_BINARY_DIR@', 'scripts'),
]

import gufi_common

class TestGUFICommon(unittest.TestCase):
    def test_get_positive(self):
        for positive in [1, 2]:
            self.assertEqual(positive, gufi_common.get_positive(str(positive)))

        for not_positive in [-1, 0]:
            with self.assertRaises(argparse.ArgumentTypeError):
                gufi_common.get_positive(str(not_positive))

        with self.assertRaises(ValueError):
            gufi_common.get_positive('')

    def test_get_non_negative(self):
        for non_negative in [0, 1]:
            self.assertEqual(non_negative, gufi_common.get_non_negative(str(non_negative)))

        for not_non_negative in [-2, -1]:
            with self.assertRaises(argparse.ArgumentTypeError):
                gufi_common.get_non_negative(str(not_non_negative))

        with self.assertRaises(ValueError):
            gufi_common.get_non_negative('')

    def test_get_char(self):
        for c in range(256):
            self.assertEqual(chr(c), gufi_common.get_char(chr(c)))

        for not_char in ['', '  ']:
            with self.assertRaises(argparse.ArgumentTypeError):
                gufi_common.get_char(not_char)

    def test_get_size(self):
        for prefix in ['-', '+']:
            for suffix in 'bcwkMG':
                gufi_common.get_size('{0}1'.format(prefix))
                gufi_common.get_size('{0}1{1}'.format(prefix, suffix))
                gufi_common.get_size('1{0}'.format(suffix))

                with self.assertRaises(argparse.ArgumentTypeError):
                    value = '{0}{1}'.format(prefix, suffix)
                    gufi_common.get_size(value)

    def test_get_user(self):
        with self.assertRaises(ValueError):
            gufi_common.get_user('')

    def test_get_group(self):
        with self.assertRaises(ValueError):
            gufi_common.get_group('')

    def test_get_port(self):
        for valid_port in range(65536):
            self.assertEqual(valid_port, gufi_common.get_port(str(valid_port)))

        for invalid_port in [-1, 65536]:
            with self.assertRaises(argparse.ArgumentTypeError):
                gufi_common.get_port(str(invalid_port))

    def test_build_query(self):
        select      = list('columns')
        table_name  = ['table']
        where       = ['c', 'o']
        group_by    = ['l', 'u']
        order_by    = ['m', 'n']
        num_results = 's'
        extra       = ';'

        expected = ''
        built = gufi_common.build_query(None, table_name, None, None, None, None, None)
        self.assertEqual(expected, built)

        built = gufi_common.build_query(select, None, None, None, None, None, None)
        self.assertEqual(expected, built)

        expected = 'SELECT {0} FROM {1}'.format(', '.join(select), ', '.join(table_name))
        built = gufi_common.build_query(select, table_name, None, None, None, None, None)
        self.assertEqual(expected, built)

        expected += (' WHERE ' + ' AND '.join(['({0})'.format(w) for w in where]) +
                     ' GROUP BY ' + ', '.join(['{0}'.format(g) for g in group_by]) +
                     ' ORDER BY ' + ', '.join(['{0}'.format(o) for o in order_by]) +
                     ' LIMIT {0}'.format(num_results) +
                     ' ' + extra)
        built = gufi_common.build_query(select, table_name, where,
                                        group_by, order_by, num_results,
                                        extra)
        self.assertEqual(expected, built)

    def test_add_common_flags(self):
        parser = argparse.ArgumentParser()
        gufi_common.add_common_flags(parser)

        args = parser.parse_args([])

        # defaults
        self.assertEqual(' ',  args.delim)
        self.assertEqual(None, args.skip)

if __name__ == '__main__':
    unittest.main()
