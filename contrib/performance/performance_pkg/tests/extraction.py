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

from performance_pkg.extraction.gufi_query import cumulative_times as gq_ct, cumulative_times_terse as gq_ctt
from performance_pkg.extraction.gufi_trace2index import cumulative_times as gt2i_ct

class TestExtraction(unittest.TestCase):
    def key_colon_value(self, module):
        columns = module.COLUMNS[3:]

        # create input
        lines = []
        for i, column in enumerate(columns):
            key, type = column                          # pylint: disable=redefined-builtin
            lines += ['{0}: {1}'.format(key, type(i))]  # pylint: disable=redefined-builtin

        # parse input
        # prefix empty line and bad line
        # sort lines to change order lines are received
        parsed = module.extract(['', columns[0][0]] + sorted(lines), None, None)

        self.assertEqual(len(parsed), len(module.COLUMNS))
        for i, column in enumerate(columns):
            key, type = column                          # pylint: disable=redefined-builtin
            self.assertEqual(parsed[key], str(type(i))) # pylint: disable=redefined-builtin

        # did not get enough values
        with self.assertRaises(ValueError):
            module.extract([], None, None)

    def test_gufi_query_cumulative_times(self):
        self.key_colon_value(gq_ct)

    def test_gufi_query_cumulative_times_terse(self):
        columns = gq_ctt.COLUMNS[3:]

        line = ' '.join(str(columns[i][1](i)) for i in range(len(columns)))

        # parse input
        # prefix empty line and bad line
        parsed = gq_ctt.extract(['', ':', line], None, None)

        self.assertEqual(len(parsed), len(gq_ctt.COLUMNS))
        for i, column in enumerate(columns):
            key, type = column                          # pylint: disable=redefined-builtin
            self.assertEqual(parsed[key], str(type(i))) # pylint: disable=redefined-builtin

        # did not find valid line
        with self.assertRaises(ValueError):
            gq_ctt.extract([], None, None)

    def test_gufi_trace2index_cumulative_times(self):
        self.key_colon_value(gt2i_ct)

if __name__ == '__main__':
    unittest.main()
