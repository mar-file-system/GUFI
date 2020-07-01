#!/usr/bin/env python2.7
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



import imp
import io
import os
import sys
import unittest

root = os.path.abspath(__file__)
for _ in xrange(4):
    root = os.path.dirname(root)
scripts = os.path.join(root, 'scripts')

# import the gufi_config in scripts
sys.path.append(scripts)
gufi_config = imp.load_source('gufi_config', os.path.join(scripts, 'gufi_config.py'))

def build_config(pairs):
    return io.BytesIO(''.join(['{}={}\n'.format(key, value) for key, value in pairs.items()]))

def build_missing(pairs, remove):
    config = ''
    for key, value in pairs.items():
        if remove != key:
            config += '{}={}\n'.format(key, value)
    return io.BytesIO(config)

class server(object):
    def setUp(self):
        self.pairs[gufi_config.Server.THREADS]      = '5'
        self.pairs[gufi_config.Server.EXECUTABLE]   = os.path.join(root, 'src', 'gufi_query')
        self.pairs[gufi_config.Server.INDEXROOT]    = root,
        self.pairs[gufi_config.Server.OUTPUTBUFFER] = '1024'

    def test_ok(self):
        try:
            gufi_config.Server(build_config(self.pairs))
        except Exception as e:
            self.fail("Reading good server configuration raised: {}".format(e))

    def test_missing_threads(self):
        with self.assertRaises(Exception):
            gufi_config.Server(build_missing(self.pairs, gufi_config.Server.THREADS))

    def test_missing_executable(self):
        with self.assertRaises(Exception):
            gufi_config.Server(build_missing(self.pairs, gufi_config.Server.EXECUTABLE))

    def test_missing_indexroot(self):
        with self.assertRaises(Exception):
            gufi_config.Server(build_missing(self.pairs, gufi_config.Server.INDEXROOT))

    def test_missing_outputbuffer(self):
        with self.assertRaises(Exception):
            gufi_config.Server(build_missing(self.pairs, gufi_config.Server.OUTPUTBUFFER))

    def test_count(self):
        config = gufi_config.Server(build_missing(self.pairs, None))
        self.assertEqual(len(config.config), len(config.SETTINGS))

class client(object):
    def setUp(self):
        self.pairs[gufi_config.Client.SERVER]   = 'hostname'
        self.pairs[gufi_config.Client.PORT]     = '22'
        self.pairs[gufi_config.Client.PARAMIKO] = 'paramiko'

    def test_ok(self):
        try:
            gufi_config.Client(build_config(self.pairs))
        except Exception as e:
            self.fail("Reading good client configuration raised: {}".format(e))

    def test_missing_server(self):
        with self.assertRaises(Exception):
            gufi_config.Client(build_missing(self.pairs, gufi_config.Client.SERVER))

    def test_missing_port(self):
        with self.assertRaises(Exception):
            gufi_config.Client(build_missing(self.pairs, gufi_config.Client.PORT))

    def test_missing_paramiko(self):
        with self.assertRaises(Exception):
            gufi_config.Client(build_missing(self.pairs, gufi_config.Client.PARAMIKO))

    def test_count(self):
        config = gufi_config.Client(build_missing(self.pairs, None))
        self.assertEqual(len(config.config), len(config.SETTINGS))

class gufi_config_server(server, unittest.TestCase):
    def setUp(self):
        self.pairs = {}
        server.setUp(self)

class gufi_config_server_combined(server, unittest.TestCase):
    def setUp(self):
        self.pairs = {}
        server.setUp(self)

        # add client configuration
        self.pairs[gufi_config.Client.SERVER]   = 'hostname'
        self.pairs[gufi_config.Client.PORT]     = '22'
        self.pairs[gufi_config.Client.PARAMIKO] = 'paramiko'

class gufi_config_client(client, unittest.TestCase):
    def setUp(self):
        self.pairs = {}
        client.setUp(self)

class gufi_config_client_combined(client, unittest.TestCase):
    def setUp(self):
        self.pairs = {}
        client.setUp(self)

        # add server configuration
        self.pairs[gufi_config.Server.THREADS]      = '5'
        self.pairs[gufi_config.Server.EXECUTABLE]   = os.path.join(root, 'src', 'gufi_query')
        self.pairs[gufi_config.Server.INDEXROOT]    = root,
        self.pairs[gufi_config.Server.OUTPUTBUFFER] = '1024'

if __name__=='__main__':
    unittest.main()
