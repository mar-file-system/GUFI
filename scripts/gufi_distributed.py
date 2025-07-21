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
import subprocess
import sys

import gufi_common

# this script assumes slurm is being used

def run_slurm(cmd, dry_run):
    if dry_run:
        return None

    query = subprocess.Popen(cmd,  # pylint: disable=consider-using-with
                             stdout=subprocess.PIPE)
    out, _ = query.communicate()   # block until cmd finishes

    if query.returncode:
        sys.exit(query.returncode)

    return out.split()[-1]         # keep slurm job id

# step 1
# run find and return sorted list of directories
def dirs_at_level(root, level):
    # can't use %P to remove input path in macos and Alpine Linux
    cmd = ['find', root, '-mindepth', str(level), '-maxdepth', str(level), '-type', 'd']
    query = subprocess.Popen(cmd,   # pylint: disable=consider-using-with
                             stdout=subprocess.PIPE)
    dirs, _ = query.communicate() # block until find finishes

    if query.returncode:
        sys.exit(query.returncode)

    return [path.decode()[len(root) + int(root[-1] != os.path.sep):]
            for path in dirs.split(b'\n') if len(path) > 0]

# step 2
# split directories into groups of unique basenames for processing
def group_dirs(dirs, splits, sort_by_basename):
    count = len(dirs)
    group_size = count // splits + int(bool(count % splits))
    ordered = sorted(dirs, key=os.path.basename) if sort_by_basename else sorted(dirs)
    return group_size, [ordered[i: i + group_size] for i in range(0, count, group_size)]

# step 3
# get only the first and last paths in each group
# print debug messages
# run function to schedule jobs if it exists
# pylint: disable=too-many-arguments,too-many-positional-arguments
def schedule_subtrees(dir_count, splits, group_size, groups,
                      group_file_prefix, schedule_subtree):
    print('Splitting {0} paths into {1} groups of max size {2}'.format(dir_count,
                                                                       splits,
                                                                       group_size))

    jobids = []
    for i, group in enumerate(groups):
        count = len(group)

        if count == 0:
            break

        print('    Range {0}: {1} path{2}'.format(i, count, 's' if count != 1 else ''))
        print('        {0} {1}'.format(group[0], group[-1]))

        if schedule_subtree:
            filename = '{0}.{1}'.format(group_file_prefix, i)
            with open(filename, 'w', encoding='utf-8') as f:
                for path in group:
                    f.write(path)
                    f.write('\n')

            jobid = schedule_subtree(i, filename)
            if jobid is not None:
                jobids += [jobid.decode()]

    return jobids

# call this inside step 4's function
def depend_on_slurm_jobids(jobids):
    return ['--dependency', 'afterok:' + ':'.join(jobids)]

# step 4
# schdule job to process top-level directories that were skipped
# after all subdirectories have been processed
def schedule_top(func, jobids):
    # func should be a wrapper around another function
    # that binds additional arguments
    return func(jobids)

# call this combined function to distribute work
# pylint: disable=too-many-arguments,too-many-positional-arguments
def distribute_work(root, level, nodes,
                    group_file_prefix, sort_by_basename,
                    schedule_subtree_func, schedule_top_func):
    dirs = dirs_at_level(root, level)
    group_size, groups = group_dirs(dirs, nodes, sort_by_basename)
    jobids = schedule_subtrees(len(dirs), nodes, group_size, groups,
                               group_file_prefix, schedule_subtree_func)
    jobids += [schedule_top(schedule_top_func, jobids).decode()]
    return jobids

def dir_arg(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError("Bad directory: {0}".format(path))
    return path

def parse_args(name, desc):
    parser = argparse.ArgumentParser(name, description=desc)

    parser.add_argument('--version', '-v',
                        action='version',
                        version=gufi_common.VERSION)

    parser.add_argument('--sbatch', metavar='exec',
                        type=str,
                        default='sbatch')

    parser.add_argument('--group_file_prefix',
                        type=str,
                        default='path_list',
                        help='prefix for file containing paths to be processed by one node')

    parser.add_argument('--sort-by-basename',
                        action='store_true',
                        help='distribute paths by basename instead of parent name')

    parser.add_argument('level',
                        type=gufi_common.get_positive,
                        help='Level at which work is distributed across nodes')

    parser.add_argument('nodes',
                        type=gufi_common.get_positive,
                        help='Number nodes to split work across')

    return parser # allow for others to use this parser
