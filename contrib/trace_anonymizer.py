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



from base64 import urlsafe_b64encode, urlsafe_b64decode
from binascii import hexlify
import argparse
import os
import sys

sys.path += ['@CMAKE_CURRENT_BINARY_DIR@']

import hashes

# trace file format
PATH_IDX = 0
UID_IDX  = 5
GID_IDX  = 6
LINK_IDX = 13

UID_MIN = 1000
UID_MAX = 1 << 32

GID_MIN = 1000
GID_MAX = 1 << 32

def anonymize(string,                                # the data to hash
              sep = os.sep,                          # character that separates paths
              hash = hashes.BuiltInHash,             # an object that has a 'digest' function
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

# convert anonymized integer column back to an integer
def anonymize_int(args, column):
    return int(hexlify(urlsafe_b64decode(anonymize(column, hash=Hashes[args.hash]))), 16)

# bound an anonymized integer within [lower, higher)
# while preventing collisions
def limit_int(args, column, used, lower, higher):
    if lower >= higher:
        raise ValueError('Lower bound ({}) of range is greater than or equal to the high bound ({})'.format(lower, higher))

    # anonymize the column
    anon = anonymize_int(args, column) % higher
    if anon < lower:
        anon = lower

    found = False
    for _ in xrange(lower, higher):
        # this is a new mapping
        if anon not in used:
            used[anon] = column
            found = True
            break

        # this value has been previously assigned to
        # the same source column, so use the value
        if used[anon] == column:
            found = True
            break

        # this value has been previously assigned to a
        # different source column, so try another value
        anon = (anon + 1) % higher
        if anon < lower:
            anon = lower

    if not found:
        raise RuntimeError('Failed to find a mapping for {} in range [{}, {})'.format(column, lower, higher))

    return str(anon)

# make sure -i and -o are only 1 character long
def char(c):
    if len(c) != 1:
        raise argparse.ArgumentTypeError("Expected 1 character. Got {}.".format(len(c)))
    return c

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Column Anonymizer: Read from stdin. Output to stdout.')
    parser.add_argument('hash',              nargs='?',        default="BuiltInHash", choices=hashes.Hashes.keys(),
                        help='Hash function name')
    parser.add_argument('-i', '--in-delim',  dest='in_delim',  default="\x1e",        type=char,
                        help='Input Column Delimiter')
    parser.add_argument('-o', '--out-delim', dest='out_delim', default="\x1e",        type=char,
                        help='Output Column Delimiter (Do not use space)')
    args = parser.parse_args()

    # mapping of anonymized ints to original ints
    used_uids = {}
    used_gids = {}

    # read lines from stdin
    for line in sys.stdin:
        while line[-1] == '\n':
            line = line[:-1]
        nl = True
        out = []
        for idx, column in enumerate(line.split(args.in_delim)):
            if idx in [PATH_IDX, LINK_IDX]:
                anon = anonymize(column, hash=Hashes[args.hash])
                if len(anon) > 490:
                    nl = False
                    break
                else:
                    out += [anon]
            elif idx == UID_IDX:
                out += [limit_int(args, column, used_uids, UID_MIN, UID_MAX)]
            elif idx == GID_IDX:
                out += [limit_int(args, column, used_gids, GID_MIN, GID_MAX)]
            else:
                out += [column]
        if nl:
            print args.out_delim.join(out)
