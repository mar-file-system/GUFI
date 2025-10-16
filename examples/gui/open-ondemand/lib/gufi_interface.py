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



from dataclasses import dataclass
from typing import List

import subprocess
import time

""" This dataclass is used to represent a file or directory entry as returned by gufi_ls. """
@dataclass
class Node:
    file_type: str
    permissions: str
    links: str
    user: str
    group: str
    size: str
    last_mod_day: str
    last_mod_month: str
    last_mod_time: str
    name: str

def run_gufi_ls(path, options: List[str] = None) -> List[Node]:
    def parse_ls_stdout(stdout):
        lines = stdout.splitlines()
        nodes = []
        for line in lines:
            parts = line.split()
            if len(parts) < 9:
                continue
            permissionString, links, user, group, size, last_mod_month, last_mod_day, last_mod_time, *name = parts
            file_type = permissionString[0]
            if file_type == '-':
                file_type = 'f'
            permissions = permissionString[1:]
            name = ' '.join(name)
            nodes.append(Node(file_type, permissions, links, user, group, size, last_mod_day, last_mod_month, last_mod_time, name))
        return nodes

    if options is None:
        options = ['-l', '-a']
    cmd = ['gufi_ls'] + options + [path]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                check=True, text=True)
        return parse_ls_stdout(result.stdout)
    except Exception as e:
        print(f"Failed to run gufi_ls with command: {' '.join(cmd)}: {e}")
        return []

gufi_find_help_text = {
        'amin': 'File was last accessed n minutes ago.',
        'atime': 'File was last accessed n*24 hours ago.',
        'cmin': "File's status was last changed n minutes ago.",
        'ctime': "File's status was last changed n*24 hours ago.",
        'empty': 'File is empty and is either a regular file or a directory.',
        'executable': 'Matches files which are executable and directories which are searchable (in a file name resolution sense).',
        # just do group 'gid': "File's numeric group ID is n.",
        'group': 'File belongs to group gname (numeric group ID allowed).',
        # remove for now'iname': 'Like name, but the match is case insensitive.',
        # remove for now 'inum': 'File has inode number n. It is normally easier to use the -samefile test instead.',
        'links': 'File has n links.',
        # remove for now 'lname': 'File is a symbolic link whose contents match shell pattern pattern.',
        'mmin': "File's data was last modified n minutes ago.",
        'mtime': "File's data was last modified n*24 hours ago.",
        'name': 'Base of file name (the path with the leading directories removed) matches shell pattern pattern.',
        'newer': 'File was modified more recently than file.',
        'path': 'File name matches shell pattern pattern.',
        'readable': 'Matches files which are readable.',
        'samefile': 'File refers to the same inode as name.',
        'size': 'File size.',
        'type': 'File is of type c.',
        # just do user'uid': "File's numeric user ID is n.",
        'user': 'File is owned by user uname (numeric user ID allowed).',
        'writable': 'Matches files which are writable.',
        'maxdepth': 'Descend at most levels (a non-negative integer) levels of directories below the command line arguments. -maxdepth 0 means only apply the tests and actions to the command line arguments.',
        'mindepth': 'Do not apply any tests or actions at levels less than levels (a non-negative integer). mindepth 1 means process all files except the command line arguments.',
        'size_percent': 'Modifier to the size flag. Expects 2 values that define the min and max percentage from the size.',
        'num-results': 'first n results',
        'smallest': 'smallest results',
        'largest': 'largest results',
    }

def run_gufi_find(path_prefix: str = "", args: dict = None):
    cmd_args = []
    if path_prefix:
        cmd_args += ['-P', path_prefix]

    double_dash = {'num-results'}
    sort_output_flags = {'smallest', 'largest'}
    boolean_flags = {'empty', 'executable', 'readable', 'writable'}

    # validate args and add to cmd_args
    for k, v in args.items():
        if k not in gufi_find_help_text:
            raise ValueError(f"Invalid gufi_find argument: {k}")
        if v not in ["None", None, '', False]:
            if k in double_dash:
                cmd_args += [f'--{k}', str(v)]
            elif k in sort_output_flags:
                cmd_args += [f'--{k}']
            elif k in boolean_flags:
                cmd_args += [f'-{k}']
            else:
                cmd_args += [f'-{k}', str(v)]

    cmd = ['gufi_find'] + cmd_args
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, check=True)
        lines = result.stdout.strip().split('\n') if result.stdout else []
        return lines
    except Exception as e:
        print(f"Failed to run gufi_find with command: {' '.join(cmd)}: {e}")
        return []

def run_gufi_stat(path):
    cmd = ['gufi_stat', '--format', '%n %s %b %o %F %D %i %h %a/%A %u %g %X %Y %Z %W\n', path]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, check=True)
        line = result.stdout.split()

        to_localtime = lambda time_str: time.strftime('%Y-%m-%d %H:%M:%S %z', time.localtime(int(time_str)))

        return {
            'File':      line[0],
            'Size':      line[1],
            'Blocks':    line[2],
            'IO Block':  line[3],
            'File type': line[4],
            'Device':    line[5],
            'Inode':     line[6],
            'Links':     line[7],
            'Mode':      line[8],
            'Uid':       line[9],
            'Gid':       line[10],
            'Access':    to_localtime(line[11]),
            'Modify':    to_localtime(line[12]),
            'Change':    to_localtime(line[13]),
            'Birth':     to_localtime(line[14]),
        }
    except Exception as e:
        print(f"Failed to run gufi_stat with command: {' '.join(cmd)}: {e}")
        return None
