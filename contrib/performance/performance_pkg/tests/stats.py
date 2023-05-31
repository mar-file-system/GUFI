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



import math
import unittest

from performance_pkg.graph import config, stats

class TestStat(unittest.TestCase): # pylint: disable=too-many-instance-attributes
    def setUp(self):
        self.even = [0, 1, 2, 3]
        self.odd  = [0, 1, 2, 3, 4]

        self.runs = 3
        self.col_count = len(self.odd)
        self.columns = self.odd
        self.single_commit_raw_numbers = [self.odd] * self.runs
        self.stat_names = [stats.AVERAGE, stats.MEDIAN,
                           stats.MINIMUM, stats.MAXIMUM]
        self.commit_count = 2
        self.commits = []
        for i in range(self.commit_count):
            cd = stats.CommitData(i, i, 0)
            cd.raw_data = self.single_commit_raw_numbers
            self.commits += [cd]

    def test_average(self):
        self.assertEqual(stats.average(self.even), 1.5)
        self.assertEqual(stats.average(self.odd),  2)

    def test_median(self):
        self.assertEqual(stats.median(self.even), 1.5)
        self.assertEqual(stats.median(self.odd),  2)

    def single_commit_stats_check(self, scs):
        # stats are listed in the returned dictionary
        for stat_name in self.stat_names:
            self.assertIn(stat_name, scs)

        # each column's stat is correct
        for c in range(self.col_count):
            self.assertEqual(scs[stats.AVERAGE][c], c)
            self.assertEqual(scs[stats.MEDIAN] [c], c)
            self.assertEqual(scs[stats.MINIMUM][c], c)
            self.assertEqual(scs[stats.MAXIMUM][c], c)

    def single_commit_nan(self, scs):
        for stat_name in self.stat_names:
            self.assertIn(stat_name, scs)

            for c in range(self.col_count):
                self.assertEqual(math.isnan(scs[stats.AVERAGE][c]), True)
                self.assertEqual(math.isnan(scs[stats.MEDIAN] [c]), True)
                self.assertEqual(math.isnan(scs[stats.MINIMUM][c]), True)
                self.assertEqual(math.isnan(scs[stats.MAXIMUM][c]), True)

    def test_single_commit_stats(self):
        scs = stats.single_commit_stats(self.columns,
                                        self.single_commit_raw_numbers,
                                        self.stat_names)

        scs_none = stats.single_commit_stats(self.columns,
                                             [[None for x in range(len(self.odd))]]*self.runs,
                                             self.stat_names)
        self.single_commit_stats_check(scs)
        self.single_commit_nan(scs_none)

    def test_bad_raw_value(self):
        with self.assertRaises(TypeError):
            stats.single_commit_stats(self.columns,
                                      self.single_commit_raw_numbers + [['abc', 1, 2, 3, 4]],
                                      self.stat_names)

    def test_multiple_commit_stats(self):
        mcs = stats.multiple_commit_stats(self.columns, self.commits,
                                          self.stat_names, True)

        self.assertEqual(len(mcs), self.commit_count)
        for scs in mcs:
            self.single_commit_stats_check(scs)

    def test_generate_line(self):
        conf = {
            config.AXES : {
                config.AXES_Y_STAT : stats.AVERAGE,
            },
            config.ERROR_BAR : {
                config.ERROR_BAR_BOTTOM : stats.MINIMUM,
                config.ERROR_BAR_TOP    : stats.MAXIMUM,
            },
        }

        lines = stats.generate_lines(conf, self.columns,
                                     self.commits, True)

        self.assertEqual(len(lines), self.col_count)

        # each line has average, min and max for self.commit_count commits
        for c in range(self.col_count):
            line = lines[c]
            for stat_name in [stats.AVERAGE, stats.MINIMUM, stats.MAXIMUM]:
                points = line[stat_name]
                for y in range(self.commit_count):
                    self.assertEqual(points[y], c)

    def test_generate_line_bad(self):
        conf = {
            config.AXES : {
                config.AXES_Y_STAT : None, # ignored
            },
            config.ERROR_BAR : {
                config.ERROR_BAR_BOTTOM : stats.MINIMUM + stats.MAXIMUM, # ValueError
                config.ERROR_BAR_TOP    : stats.MAXIMUM,
            },
        }

        with self.assertRaises(ValueError):
            stats.generate_lines(conf, self.columns, self.commits, True)

if __name__ == '__main__':
    unittest.main()
