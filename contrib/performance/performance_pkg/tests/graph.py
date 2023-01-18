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
import io
import tempfile
import unittest

# https://stackoverflow.com/a/4935945
# by Joe Kington
import matplotlib as mpl
mpl.use('Agg')

from performance_pkg.graph import config, graph, stats # pylint: disable=wrong-import-position

class TestConfig(unittest.TestCase):
    def test_pos_float(self):
        with self.assertRaises(ValueError):
            config.pos_float('-1')
        with self.assertRaises(ValueError):
            config.pos_float('-1.0')
        with self.assertRaises(ValueError):
            config.pos_float('0')
        with self.assertRaises(ValueError):
            config.pos_float('0.0')
        self.assertEqual(1, config.pos_float('1'))
        self.assertEqual(1, config.pos_float('1.0'))
        with self.assertRaises(ValueError):
            config.pos_float('abc')

    def test_to_list(self):
        self.assertEqual([''], config.to_list(''))
        self.assertEqual(['a', 'b'], config.to_list('a, b'))
        self.assertEqual(['a', 'b', ''], config.to_list('a, b,'))

    def test_str_list(self):
        self.assertEqual([''], config.str_list(''))
        self.assertEqual(['a', 'b'], config.str_list('a, b'))
        self.assertEqual(['a', 'b', ''], config.str_list('a, b,'))

    def test_pos_float_list(self):
        with self.assertRaises(ValueError):
            config.pos_float_list('-1')
        with self.assertRaises(ValueError):
            config.pos_float_list('-1.0')
        with self.assertRaises(ValueError):
            config.pos_float_list('0')
        with self.assertRaises(ValueError):
            config.pos_float_list('0.0')
        self.assertEqual([1.0], config.pos_float_list('1'))
        self.assertEqual([1.0], config.pos_float_list('1.0'))
        with self.assertRaises(ValueError):
            config.pos_float_list('a')

    def test_default_config(self):
        with tempfile.NamedTemporaryFile() as cfg:
            conf = config.config_file(cfg.name)

            for section, keys in config.DEFAULTS.items():
                for key, settings in keys.items():
                    _, expected = settings
                    self.assertEqual(expected, conf[section][key])

    def test_override_args(self):
        parser = argparse.ArgumentParser()
        config.override_args(parser)
        args = parser.parse_args([])

        for section, keys in config.DEFAULTS.items():
            for key, _ in keys.items():
                attr = config.override_name(section, key)
                self.assertTrue(hasattr(args, attr))

    def test_override(self):
        str_override = '-'
        num_override = '1'

        class Fake: # pylint: disable=too-few-public-methods
            pass

        # fake args
        args = Fake()
        for section, keys in config.DEFAULTS.items():
            for key, settings in keys.items():
                convert, _ = settings
                attr = config.override_name(section, key)
                if convert in [str, config.str_list]:
                    setattr(args, attr, convert(str_override))
                elif convert in [int, float, bool]:
                    setattr(args, attr, convert(num_override))
                else:
                    setattr(args, attr, None)

        with tempfile.NamedTemporaryFile() as cfg:
            conf = config.override(config.config_file(cfg.name), args)
            for section, keys in config.DEFAULTS.items():
                for key, settings in keys.items():
                    convert, _ = settings
                    if convert in [str]:
                        self.assertEqual(str_override, conf[section][key])
                    elif convert in [config.str_list]:
                        self.assertEqual([str_override], conf[section][key])
                    elif convert in [int, float, bool]:
                        self.assertEqual(convert(num_override), conf[section][key])

class TestGraph(unittest.TestCase):
    def test_generate(self):
        # set up configuration
        conf = {section : {key : setting[1]
                           for key, setting in keys.items()}
                for section, keys in config.DEFAULTS.items()}

        conf[config.RAW_DATA][config.RAW_DATA_COLUMNS] = ['column'] # single line
        conf[config.OUTPUT][config.OUTPUT_PATH] = io.BytesIO()      # in-memory graph
        conf[config.OUTPUT][config.OUTPUT_DIMENSIONS] = [1, 1]
        conf[config.AXES][config.AXES_X_LABEL_ROTATION] = 0
        conf[config.AXES][config.AXES_X_LABEL_SIZE] = 10
        conf[config.AXES][config.AXES_Y_STAT] = stats.AVERAGE
        conf[config.AXES][config.AXES_Y_MIN] = -10
        conf[config.AXES][config.AXES_Y_MAX] = 10
        conf[config.AXES][config.AXES_ANNOTATE] = True
        conf[config.ERROR_BAR][config.ERROR_BAR_BOTTOM] = stats.MINIMUM
        conf[config.ERROR_BAR][config.ERROR_BAR_TOP] = stats.MAXIMUM
        conf[config.ERROR_BAR][config.ERROR_BAR_ANNOTATE] = True

        points = 5 # number of commits/points in each line
        commits = ['commit'] * points
        lines = {
            'column' : {
                stats.AVERAGE : list(range(points)),
                stats.MINIMUM : list(range(points)),
                stats.MAXIMUM : list(range(points)),
            },
        }

        try:
            graph.generate(conf, commits, lines)
        except Exception as err: # pylint: disable=broad-except
            self.fail('Graphing test raised: {0}'.format(err))

if __name__ == '__main__':
    unittest.main()
