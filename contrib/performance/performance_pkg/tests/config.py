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
import tempfile
import unittest

from performance_pkg.graph import config

class TestConfig(unittest.TestCase):
    def test_pos_int(self):
        with self.assertRaises(ValueError):
            config.pos_int('-1')
        with self.assertRaises(ValueError):
            config.pos_int('0')
        self.assertEqual(1, config.pos_int('1'))
        with self.assertRaises(ValueError):
            config.pos_int('abc')

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
        override = '' # any non-None value

        class Temp: # pylint: disable=too-few-public-methods
            pass

        # fake args
        args = Temp()
        for section, keys in config.DEFAULTS.items():
            for key, _ in keys.items():
                attr = config.override_name(section, key)
                setattr(args, attr, override)

        with tempfile.NamedTemporaryFile() as cfg:
            conf = config.override(config.config_file(cfg.name), args)

            for section, keys in config.DEFAULTS.items():
                for key, _ in keys.items():
                    self.assertEqual(override, conf[section][key])

if __name__ == '__main__':
    unittest.main()
