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



#!/usr/bin/env python2

import argparse
import sys

def generate_level_r(out, parent, dir_count, file_count, current_level, max_level, rs = chr(0x1e)):
    # generate all files first
    for f_name in xrange(file_count):
        out.write(parent + 'f.' + str(f_name) + rs + 'f' + rs + '1' + rs + '33188' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs + '\n')

    for d_name in xrange(dir_count):
        out.write(parent + 'd.' + str(d_name) + rs + 'd' + rs + '1' + rs + '16832' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs +'\n')

        if current_level < max_level:
            # generate each directory with its children
            generate_level_r(out, parent + 'd.' + str(d_name) + '/', dir_count, file_count, current_level + 1, max_level, rs)

def generate_level(out, root, dir_count, file_count, max_level, rs = chr(0x1e)):
    # start at root
    out.write(root + rs + 'd' + rs + '1' + rs + '16832' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs +'\n')
    generate_level_r(args.output, root, args.directories, args.files, 1, args.depth, args.separator)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Trace Generator')
    parser.add_argument('output',      type=argparse.FileType('w+b'), help='output file')
    parser.add_argument('directories', type=int,                      help='number of directories per directory')
    parser.add_argument('files',       type=int,                      help='number of files per directory')
    parser.add_argument('depth',       type=int,                      help='number of levels')
    parser.add_argument('--separator', type=str, default=chr(0x1e),   help='record separator')
    args = parser.parse_args()

    generate_level(args.output, '/', args.directories, args.files, args.depth, args.separator)
