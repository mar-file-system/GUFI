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
import unittest
import sys

# import the gufi_config in scripts
sys.path.append('@CMAKE_BINARY_DIR@/scripts')
gufi_config = imp.load_source('gufi_config', '@CMAKE_BINARY_DIR@/scripts/gufi_config.py')

def build_config(pairs):
    return io.BytesIO(''.join(['{}={}\n'.format(key, value) for key, value in pairs.items()]))

def build_missing(pairs, remove):
    config = ''
    for key, value in pairs.items():
        if remove != key:
            config += '{}={}\n'.format(key, value)
    return io.BytesIO(config)

class gufi_config_server(unittest.TestCase):
    def setUp(self):
        self.pairs = {gufi_config.Server.THREADS      : '5',
                      gufi_config.Server.EXECUTABLE   : '@CMAKE_BINARY_DIR@/src/gufi_query',
                      gufi_config.Server.INDEXROOT    : '@CMAKE_BINARY_DIR@',
                      gufi_config.Server.OUTPUTBUFFER : '1024'}

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

class gufi_config_client(unittest.TestCase):
    def setUp(self):
        self.pairs = {gufi_config.Client.SERVER   : 'hostname',
                      gufi_config.Client.PORT     : '22',
                      gufi_config.Client.PARAMIKO : 'paramiko'}

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

if __name__=='__main__':
    unittest.main()
