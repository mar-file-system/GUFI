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
import os
import sys

import gufi_common

# configuration file location
DEFAULT_PATH = '/etc/GUFI/config'

# helper function to allow configurable paths to the gufi config file
def config_path():
    gufi_config_env = 'GUFI_CONFIG'
    if gufi_config_env in os.environ:
        return os.environ[gufi_config_env]
    return DEFAULT_PATH

class Config(object): # pylint: disable=too-few-public-methods,useless-object-inheritance
    def __init__(self, settings, config_reference=DEFAULT_PATH):
        # path string
        if isinstance(config_reference, str):
            with open(config_reference, 'r') as config_file: # pylint: disable=unspecified-encoding
                self.config = self._read_lines(settings, config_file, config_reference)
        # iterable object containing lines
        elif self._check_iterable(config_reference):
            self.config = self._read_lines(settings, config_reference, config_reference)
        else:
            raise TypeError('Cannot convert {0} to a config'.format(type(config_reference)))

    @staticmethod
    def _check_iterable(obj):
        try:
            iter(obj)
        except TypeError:
            return False
        return True

    @staticmethod
    def _read_lines(settings, lines, path):
        out = {}
        for line in lines:
            line = line.strip()

            if len(line) == 0:
                continue

            if line[0] == '#':
                continue

            key, value = line.split('=', 1)

            # only store known keys
            if key in settings:
                # some values need to be parsed
                if settings[key]:
                    out[key] = settings[key](value)
                else:
                    out[key] = value

        for key in settings:
            if key not in out:
                raise KeyError('While attempting to parse GUFI config at {0} found missing setting {1}'.format(path, key))

        return out

class Server(Config):
    THREADS      = 'Threads'      # number of threads to use
    QUERY        = 'Query'        # absolute path of gufi_query
    STAT         = 'Stat'         # absolute path of gufi_stat_bin
    INDEXROOT    = 'IndexRoot'    # absolute path of root directory for GUFI to traverse
    OUTPUTBUFFER = 'OutputBuffer' # size of per-thread buffers used to buffer prints

    # key -> str to value converter
    SETTINGS = {
        THREADS      : gufi_common.get_positive,
        QUERY        : os.path.normpath,
        STAT         : os.path.normpath,
        INDEXROOT    : os.path.normpath,
        OUTPUTBUFFER : gufi_common.get_non_negative
    }

    def __init__(self, config_reference):
        # pylint: disable=super-with-arguments
        super(Server, self).__init__(Server.SETTINGS, config_reference)

    def threads(self):
        '''return number of threads to use'''
        return self.config[Server.THREADS]

    def query(self):
        '''return absolute path of gufi_query'''
        return self.config[Server.QUERY]

    def stat(self):
        '''return absolute path of gufi_stat_bin'''
        return self.config[Server.STAT]

    def indexroot(self):
        '''return absolute path of root directory for GUFI to traverse'''
        return self.config[Server.INDEXROOT]

    def outputbuffer(self):
        '''return size of per-thread buffers used to buffer prints'''
        return self.config[Server.OUTPUTBUFFER]

class Client(Config):
    SERVER       = 'Server'       # hostname
    PORT         = 'Port'         # ssh port
    PARAMIKO     = 'Paramiko'     # location of paramiko installation

    # key -> str to value converter
    SETTINGS = {
        SERVER   : None,
        PORT     : gufi_common.get_port,
        PARAMIKO : os.path.normpath,
    }

    def __init__(self, config_reference):
        # pylint: disable=super-with-arguments
        super(Client, self).__init__(Client.SETTINGS, config_reference)

    def server(self):
        '''return hostname of server'''
        return self.config[Client.SERVER]

    def port(self):
        '''return ssh port'''
        return self.config[Client.PORT]

    def paramiko(self):
        '''return location of paramiko installation'''
        return self.config[Client.PARAMIKO]

def run(args):
    # simple config validator
    parser = argparse.ArgumentParser(description='GUFI Configuration Tester')
    parser.add_argument('type', choices=['server', 'client'])
    parser.add_argument('path')
    args = parser.parse_args(args)

    with open(args.path, 'r') as config: # pylint: disable=unspecified-encoding
        if args.type == 'server':
            Server(config)
        elif args.type == 'client':
            Client(config)

        # won't print on error
        print('Valid Config') # pylint: disable=superfluous-parens

if __name__ == '__main__':
    run(sys.argv[1:])
