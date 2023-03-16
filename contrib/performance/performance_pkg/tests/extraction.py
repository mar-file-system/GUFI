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



import sqlite3
import unittest

from performance_pkg.extraction import common
from performance_pkg.extraction.gufi_query import cumulative_times as gq_ct, cumulative_times_terse as gq_ctt
from performance_pkg.extraction.gufi_trace2index import cumulative_times as gt2i_ct

class TestExtraction(unittest.TestCase):
    # pylint: disable=redefined-builtin
    def key_value_test(self, module, separator, columns):
        # create valid input
        lines = ['{0}{1} {2}'.format(key, separator, type(i))
                 for i, (key, type) in enumerate(columns.items())]

        # use a known column name as a bad value
        bad = list(columns.keys())[0]

        # parse input (no errors despite bad inputs)
        parsed = module.extract(['',                                     # empty line (ignored)
                                 bad,                                    # column name only (bad pattern)
                                 '{0}{1} 2'.format(bad[:-1], separator), # reduced column name (not valid column name, despite substring match)
                                 '{0} {0}{1} 2'.format(bad, separator),  # extended column name (not valid column name, despite substring match)
                             ] + sorted(lines),                          # sort lines to change order lines are received
                                None, None)

        self.assertEqual(len(parsed), len(columns) + 3)                  # +3 for id, commit, and branch
        for i, (key, type) in enumerate(columns.items()):
            self.assertEqual(parsed[key], str(type(i)))

        # did not get enough values
        with self.assertRaises(ValueError):
            module.extract([], None, None)

    def test_gufi_query_cumulative_times(self):
        for column_format in gq_ct.COLUMN_FORMATS:
            self.key_value_test(gq_ct, ':', column_format)
            self.key_value_test(gq_ct, '',  column_format)

    def test_gufi_query_cumulative_times_terse(self):
        columns = gq_ctt.COLUMNS[3:]

        line = ' '.join(str(type(i)) for i, (key, type) in enumerate(columns))

        # parse input
        # prefix empty line and bad line
        parsed = gq_ctt.extract(['', ':', line], None, None)

        self.assertEqual(len(parsed), len(gq_ctt.COLUMNS))
        for i, (key, type) in enumerate(columns):
            self.assertEqual(parsed[key], str(type(i)))

        # did not find valid line
        with self.assertRaises(ValueError):
            gq_ctt.extract([], None, None)

    def test_gufi_trace2index_cumulative_times(self):
        for column_format in gt2i_ct.COLUMN_FORMATS:
            self.key_value_test(gt2i_ct, ':', column_format)

class TestCommon(unittest.TestCase):
    def test(self):
        table_name = 'table_name'

        # known format
        columns = {
            'None': None,
            'str':  str,
            'int':  int,
        }

        # read from input
        parsed = {
            'None': None,
            'str' : '',
            'int' : 0,
        }

        try:
            db = sqlite3.connect(":memory:")
            common.create_table(db, table_name, columns)
            common.insert(db, parsed, table_name, columns)
        except Exception as err: # pylint: disable=broad-except
            self.fail('Testing extraction common functions raised: {0}'.format(err))
        finally:
            db.close()

if __name__ == '__main__':
    unittest.main()
