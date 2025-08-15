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



import copy
import os
import sys
import unittest

sys.path += [
    # test/regression/common.py
    os.path.join('@CMAKE_BINARY_DIR@', 'test'),

    # scripts/gufi_config.py imports scripts/gufi_common.py
    os.path.join('@CMAKE_BINARY_DIR@', 'scripts'),
]

import gufi_config

def build_config(pairs, remove=None):
    return ['{0}={1}\n'.format(key, value) for key, value in pairs.items() if remove != key]

class TestConfig(unittest.TestCase):
    def test_constructor(self):
        with self.assertRaises(TypeError):
            gufi_config.Config([], None)

class TestServerConfig(unittest.TestCase):
    default = {
        gufi_config.Server.THREADS      : 5,
        gufi_config.Server.QUERY        : os.path.join('@CMAKE_BINARY_DIR@', 'src', 'gufi_query'),
        gufi_config.Server.SQLITE3      : os.path.join('@CMAKE_BINARY_DIR@', 'src', 'gufi_sqlite3'),
        gufi_config.Server.STAT         : os.path.join('@CMAKE_BINARY_DIR@', 'src', 'gufi_stat'),
        gufi_config.Server.INDEXROOT    : '@CMAKE_BINARY_DIR@',
        gufi_config.Server.OUTPUTBUFFER : 1024,
    }

    def setUp(self): # pylint: disable=invalid-name
        self.pairs = copy.deepcopy(TestServerConfig.default)

    def check_values(self, config):
        self.assertEqual(TestServerConfig.default[gufi_config.Server.THREADS],
                         config.threads)
        self.assertEqual(TestServerConfig.default[gufi_config.Server.QUERY],
                         config.query)
        self.assertEqual(TestServerConfig.default[gufi_config.Server.SQLITE3],
                         config.sqlite3)
        self.assertEqual(TestServerConfig.default[gufi_config.Server.STAT],
                         config.stat)
        self.assertEqual(TestServerConfig.default[gufi_config.Server.INDEXROOT],
                         config.indexroot)
        self.assertEqual(TestServerConfig.default[gufi_config.Server.OUTPUTBUFFER],
                         config.outputbuffer)

    def test_iterable(self):
        try:
            config = gufi_config.Server(build_config(self.pairs) + ['', '#'])
        except Exception as err: # pylint: disable=broad-except
            self.fail('Reading good server configuration raised: {0}'.format(err))

        self.check_values(config)

    def test_missing(self):
        for key in TestServerConfig.default:
            with self.assertRaises(KeyError):
                gufi_config.Server(build_config(self.pairs, key))

    def bad_int(self, key, badvalues):
        bad = copy.deepcopy(self.pairs)

        for badvalue in badvalues:
            bad[key] = badvalue

            with self.assertRaises(Exception):
                gufi_config.Server(build_config(bad))

    def test_bad_threads(self):
        self.bad_int(gufi_config.Server.THREADS, ['-1', '0', '', 'abc'])

    def test_bad_outputbuffer(self):
        self.bad_int(gufi_config.Server.OUTPUTBUFFER, ['-1', '', 'abc'])

class TestClientConfig(unittest.TestCase):
    default = {
        gufi_config.Client.SERVER   : 'hostname',
        gufi_config.Client.PORT     : 22,
    }

    def setUp(self):
        self.pairs = copy.deepcopy(TestClientConfig.default)

    def check_values(self, config):
        self.assertEqual(TestClientConfig.default[gufi_config.Client.SERVER],
                         config.server)
        self.assertEqual(TestClientConfig.default[gufi_config.Client.PORT],
                         config.port)

    def test_iterable(self):
        try:
            config = gufi_config.Client(build_config(self.pairs) + ['', '#'])
        except Exception as err: # pylint: disable=broad-except
            self.fail('Reading good client configuration raised: {0}'.format(err))

        self.check_values(config)

    def test_missing(self):
        for key in TestClientConfig.default:
            with self.assertRaises(KeyError):
                gufi_config.Client(build_config(self.pairs, key))

    def bad_int(self, key, badvalues):
        bad = copy.deepcopy(self.pairs)

        for badvalue in badvalues:
            bad[key] = badvalue

            with self.assertRaises(Exception):
                gufi_config.Client(build_config(bad))

    def test_bad_port(self):
        self.bad_int(gufi_config.Client.PORT, ['-1', '65536', '', 'abc'])

class TestServerConfigCombined(TestServerConfig):
    def setUp(self):
        # pylint: disable=super-with-arguments
        super(TestServerConfigCombined, self).setUp()

        # add client configuration
        self.pairs.update(TestClientConfig.default)

class TestClientConfigCombined(TestClientConfig):
    def setUp(self):
        # pylint: disable=super-with-arguments
        super(TestClientConfigCombined, self).setUp()

        # add server configuration
        self.pairs.update(TestServerConfig.default)

# make sure example configuration files are valid
#
# if example configuration files are not present
# in build root directory, reconfigure cmake
class TestConfigFile(unittest.TestCase):
    def test_server(self):
        try:
            gufi_config.run(['server', os.path.join('@CMAKE_BINARY_DIR@', 'server.example')])
        except Exception as err: # pylint: disable=broad-except
            self.fail('Reading good server configuration file raised: {0}'.format(err))

    # not testing client.example
    # CMake @CLIENT@ variable can be anything CMake accepts as a boolean

if __name__ == '__main__':
    unittest.main()
