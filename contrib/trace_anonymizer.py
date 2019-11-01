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



#!/usr/bin/env python

from base64 import urlsafe_b64encode, urlsafe_b64decode
from binascii import hexlify, unhexlify
import argparse
import hashlib
import os
import sys

# wrapper for the built-in hash function to act as a hashlib hash class
# update() and hexdigest() are not provided
class BuiltInHash:
    def __init__(self, string):
        self.hashed = hex(abs(__builtins__.hash(string)))[2:]
        if len(self.hashed) & 1:
            self.hashed = '0' + self.hashed
        self.hashed = unhexlify(self.hashed)

    def digest(self):
        return self.hashed

# Known hashes
Hashes = {
    'BuiltInHash' : BuiltInHash,
    'md5'         : hashlib.md5,
    'sha1'        : hashlib.sha1,
    'sha224'      : hashlib.sha224,
    'sha256'      : hashlib.sha256,
    'sha384'      : hashlib.sha384,
    'sha512'      : hashlib.sha512,
}

def anonymize(string,                                # the data to hash
              sep = os.sep,                          # character that separates paths
              hash = BuiltInHash,                    # an object that has a 'digest' function
              salt = lambda string, extra_args : "", # a function to salt the each piece of the data, if necessary
              args = None):                          # extra arguments to pass into the salt function
    '''
    Splits the input into chunks, if the input contains path separators; otherwise uses the entire input as one chunk
        Empty strings are not hashed, since they are just path separators
    Salts the chunk, if necessary
    Hashes the chunk into a string
    Converts the hash into Base64 to make it printable characters only
    Recombines the chunks with path separators
    '''
    return sep.join(['' if part == '' else urlsafe_b64encode(hash((salt(part, args) if salt else "") + part).digest()) for part in string.split(sep)])

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Column Anonymizer: Read from stdin. Output to stdout.')
    parser.add_argument('hash',    nargs='?',                   default="BuiltInHash", choices=Hashes.keys(), help='Hash function name')
    parser.add_argument('-i', '--in-delim',   dest='in_delim',  default="\x1e",                               help='Input Column Delimiter')
    parser.add_argument('-o', '--out-delim',  dest='out_delim', default="\x1e",                               help='Output Column Delimiter (Do not use space)')
    args = parser.parse_args()

    # read lines from stdin
    for line in sys.stdin:
        while line[-1] == '\n':
            line = line[:-1]
        nl = True
        out = []
        for idx,column in enumerate(line.split(args.in_delim)):
            #path and linkname
            if idx in [0,13]: # path
                anon = anonymize(column, hash=Hashes[args.hash])
                if len(anon) > 490:
                    nl = False
                    break
                else:
                    out += [anon]
            #leave in case we anony ints
            elif idx in []:
                anon = anonymize(column, hash=Hashes[args.hash])
                # convert numeric columns back to numbers
                out += [str(int(hexlify(urlsafe_b64decode(anon)), 16))]
            else:
                out += [column]
        if nl:
            print args.out_delim.join(out)# + args.out_delim
