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
import os
import shlex
import subprocess
import sys
import time

import gufi_common

# how to sort paths found by find(1) at given level
SORT_DIRS = {
    'unsorted' : lambda dirs : dirs,
    'path'     : lambda dirs : sorted(dirs), # pylint: disable=unnecessary-lambda
    'basename' : lambda dirs : sorted(dirs, key = os.path.basename),
}

# wait on sbatch, not the actual job
def run_slurm(args, target, cmd):
    # pylint: disable=consider-using-with
    proc = subprocess.Popen([args.sbatch, '--nodes=1', '--nodelist={0}'.format(target)] + cmd,
                            stdout=subprocess.PIPE,
                            cwd=os.getcwd())
    out, _ = proc.communicate()
    return out.split()[-1].decode()

# wait for all jobs (not sbatch) to complete
def handle_slurm_procs(args, jobids):
    # pylint: disable=consider-using-with
    wait = subprocess.Popen([args.sbatch, '--nodes=1', '--nodelist={0}'.format(args.hosts[1]),
                             '--dependency', 'after:' + ':'.join(jobids), '--wait', '/dev/stdin'],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL, # Python 3.3
                            shell=True)

    # split shebang so shellcheck script doesn't see this
    wait.communicate(input=' '.join(['#!/usr/bin/env', 'bash']).encode('utf-8'))

    return jobids

# start jobs, but don't wait on them
def run_ssh(args, target, cmd):
    # can't return pid because that would result
    # in waiting for the process to complete
    # pylint: disable=consider-using-with
    return subprocess.Popen([args.ssh, target, 'cd', os.getcwd(), '&&'] + [shlex.quote(argv) for argv in cmd],
                            cwd=os.getcwd())

# wait for all jobs to complete
def handle_ssh_procs(_args, procs):
    jobids = []
    for proc in procs:
        proc.wait()

        if proc.returncode != 0:
            continue

        jobids += [proc.pid]

    return jobids

DISTRIBUTORS = {
    'slurm' : (run_slurm, handle_slurm_procs),
    'ssh'   : (run_ssh,   handle_ssh_procs),
}

# Step 1
# Run find(1) and return sorted list of directories
def dirs_at_level(root, level):
    # can't use %P to remove input path in macos and Alpine Linux
    cmd = ['find', '-H', root, '-mindepth', str(level), '-maxdepth', str(level), '-type', 'd']
    proc = subprocess.Popen(cmd, # pylint: disable=consider-using-with
                            stdout=subprocess.PIPE,
                            cwd=os.getcwd())
    dirs, _ = proc.communicate() # block until find finishes

    if proc.returncode:
        sys.exit(proc.returncode)

    return [path.decode()[len(root) + int(root[-1] != os.path.sep):]
            for path in dirs.split(b'\n') if len(path) > 0]

# Step 2
# Split directories into groups of paths for processing
def group_dirs(dirs, splits, sort):
    count = len(dirs)
    group_size = count // splits + int(bool(count % splits))
    ordered = SORT_DIRS[sort](dirs)
    return group_size, [ordered[i: i + group_size] for i in range(0, count, group_size)]

# Step 3
# Write the group to a per-node file and start jobs
def schedule_subtrees(args, dir_count, group_size, groups, subtree_cmd):
    targets = args.hosts[0]
    print('Splitting {0} paths into {1} groups of max size {2}'.format(dir_count,
                                                                       len(targets),
                                                                       group_size))

    procs = []
    for i, (group, target) in enumerate(zip(groups, targets)):
        count = len(group)

        if count == 0:
            break

        print('    Range {0}: {1} path{2} on {3}'.format(i, count, 's' if count != 1 else '', target))
        print('        {0} {1}'.format(group[0], group[-1]))

        if args.dry_run:
            continue

        # write group to per-node file
        filename = os.path.realpath('{0}.{1}'.format(args.group_file_prefix, i))
        with open(filename, 'w', encoding='utf-8') as f:
            for path in group:
                f.write(path)
                f.write('\n')

        cmd = subtree_cmd(args, filename, i, target)

        # run the command to process the subtree
        procs += [DISTRIBUTORS[args.distributor][0](args, target, cmd)]

    return procs

# Step 4
# Schedule job to process top-level directories that were skipped,
# after all subdirectories have been processed. Then wait on the
# top-level job for completion. Waiting for the subtrees to be
# processed is optional, but is done in order to match behaviors of
# slurm and ssh.
def schedule_top(args, func):
    target = args.hosts[1]
    cmd = func(args, target)

    print("    Process upper directories up to and including level {0} on {1}".format(args.level - 1, target))

    if args.dry_run:
        return None

    return DISTRIBUTORS[args.distributor][0](args, target, cmd)

def clock():
    # assume Python 3+
    if sys.version_info.minor < 3:
        return time.time()
    if sys.version_info.minor < 7:
        return time.monotonic() # Python 3.3
    return time.monotonic_ns()  # Python 3.7

def clock_diff(start, end):
    # assume Python 3+
    diff = end - start
    if sys.version_info.minor >= 7:
        diff /= 1e9
    return diff

# call this combined function to distribute work
def distribute_work(args, root, schedule_subtree_func, schedule_top_func):
    start = clock()

    dirs = dirs_at_level(root, args.level)
    group_size, groups = group_dirs(dirs, len(args.hosts[0]), args.sort)

    # launch jobs in parallel
    procs = schedule_subtrees(args, len(dirs), group_size, groups,
                              schedule_subtree_func)

    # process the top levels
    procs += [schedule_top(args, schedule_top_func)]

    print('Waiting for {0} jobs to complete'.format(args.distributor))

    # wait for actual jobs to return
    jobids = DISTRIBUTORS[args.distributor][1](args, procs)

    end = clock()

    print('Jobs completed in {0} seconds'.format(clock_diff(start, end)))

    return jobids

# argparse type for existing directories (i.e. source tree)
def dir_arg(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('Bad directory: {0}'.format(path))

    return path

# argparse type for nonexistant directories (i.e. target directories)
def new_dir_arg(path):
    return os.path.realpath(path)

# argparse type for executable file (symlinks are allowed)
def is_exec_file(path):
    if (os.path.isfile(path) or os.path.islink(path)) and os.access(path, os.X_OK):
        return os.path.realpath(path)

    raise argparse.ArgumentTypeError('Bad executable file: {0}'.format(path))

# read hostfile and return tuple containing
# nodes for processing tree distributed at given level
# and top levels
def read_hostfile(filename):
    with open(filename, 'r', encoding='utf-8') as hostfile:
        nodes = []
        for node in hostfile:
            node = node.strip()
            if len(node):
                nodes += [node]

    if len(nodes) < 2:
        raise RuntimeError('Need at least 2 nodes in node list')

    # all but last nodes are used for processing subtrees
    return nodes[:-1], nodes[-1]

def parse_args(name, desc):
    parser = argparse.ArgumentParser(name, description=desc)

    parser.add_argument('--version', '-v',
                        action='version',
                        version=gufi_common.VERSION)

    parser.add_argument('--dry-run',             action='store_true')

    parser.add_argument('--group_file_prefix',   metavar='path',
                        type=str,
                        default='path_list',
                        help='prefix for file containing paths to be processed by one node')

    parser.add_argument('--sort',
                        choices=SORT_DIRS.keys(),
                        default='path',
                        help='sort paths discovered by find at given level')

    # not using subparser
    parser.add_argument('distributor',
                        choices=DISTRIBUTORS)

    parser.add_argument('--sbatch',              metavar='exec',
                        type=str,
                        default='sbatch')

    parser.add_argument('--ssh',                 metavar='exec',
                        type=str,
                        default='ssh')

    parser.add_argument('hosts',                 metavar='hostfile',
                        type=read_hostfile,
                        help='file containing one path (without starting prefix) per line')

    parser.add_argument('level',
                        type=gufi_common.get_positive,
                        help='Level at which work is distributed across nodes')


    return parser # allow for others to use this parser
