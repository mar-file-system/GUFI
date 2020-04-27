#!/usr/bin/env python3
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
import imp
import os

import gufi_common

# default configuration file location
DEFAULT_PATH='/etc/GUFI/config'

def _read_file(settings, f):
    out = {}
    for line in f:
        line.strip()
        if line[-1] == '\n':
            line = line[:-1]

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
            raise Exception('Missing ' + key)

    return out

def _read_filename(settings, filename = DEFAULT_PATH):
    with open(filename, 'r') as f:
        return _read_file(settings, f)

def _check_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True

class Server:
    THREADS      = 'Threads'      # number of threads to use
    EXECUTABLE   = 'Executable'   # absolute path of gufi_query
    INDEXROOT    = 'IndexRoot'    # absolute path of root directory for GUFI to traverse
    OUTPUTBUFFER = 'OutputBuffer' # size of per-thread buffers used to buffer prints

    SETTINGS = {
        THREADS      : gufi_common.get_positive,
        EXECUTABLE   : None,
        INDEXROOT    : os.path.normpath,
        OUTPUTBUFFER : gufi_common.get_non_negative
    }

    def __init__(self, file_reference):
        if isinstance(file_reference,  str):
            self.config = _read_filename(Server.SETTINGS, file_reference)
        elif _check_iterable(file_reference):
            self.config = _read_file(Server.SETTINGS, file_reference)
        else:
            raise exception.TypeError('Cannot convert {} to a Server config'.format(type(file_reference)))

    def threads(self):
        return self.config[Server.THREADS]

    def executable(self):
        return self.config[Server.EXECUTABLE]

    def indexroot(self):
        return self.config[Server.INDEXROOT]

    def outputbuffer(self):
        return self.config[Server.OUTPUTBUFFER]

class Client:
    SERVER       = 'Server'       # hostname
    PORT         = 'Port'         # ssh port
    PARAMIKO     = 'Paramiko'     # location of paramiko installation

    SETTINGS = {
        SERVER   : None,
        PORT     : gufi_common.get_port,
        PARAMIKO : os.path.normpath,
    }

    def __init__(self, file_reference):
        if isinstance(file_reference,  str):
            self.config = _read_filename(Client.SETTINGS, file_reference)
        elif _check_iterable(file_reference):
            self.config = _read_file(Client.SETTINGS, file_reference)
        else:
            raise exception.TypeError('Cannot convert {} to a Client config'.format(type(file_reference)))

    def server(self):
        return self.config[Client.SERVER]

    def port(self):
        return self.config[Client.PORT]

    def paramiko(self):
        return self.config[Client.PARAMIKO]
