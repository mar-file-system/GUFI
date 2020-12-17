#!/usr/bin/env python2
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

import gufi_query
import gufi_trace2index

class ExecInfo:
    '''
    Holds executable specific command line argument types and
    performance data types and how to handle new information.
    '''
    def __init__(self, configuration, table_name, columns, gen, parse_output):
        '''
        Parameters:

            configuration : list of Column.Column
                List of command line arguments

            table_name    : str
                Name of table containing raw numbers

            column_name   : list of Column.Column
                List of columns stored in table_name.
                Should match the timings that are
                collected from the executable.

            gen           : function
                Generates values for running the executable, parsing the
                output, and inserting the values into the raw numbers
                table

                Parameters:
                    cursor      : sqlite3.db.cursor
                        Handle to the database

                    config_hash : str
                        The hash of the configuration to run the
                        executable with. Does not have to be a
                        full hash string but, must match with
                        exactly 1 configuration hash.

                    executable: str
                        Path of the executable to run

                Returns:
                    - A list constructed from the configuration that
                      can be executed with subprocess.Popen

                    - The full configuration hash that matched
                      config_hash

                    - Any extra information that is needed to parse
                      the output

            parse_output  : function
                Parses the output returned by the executable

                Parameters:
                    lines : str
                        Ahe lines of output in one big string

                    extra : any
                        Any extra arguments that are needed to parse
                        the lines

                Return:
                    List of values extracted from the output in the
                    same order the columns are listed in.  The only
                    requirement for each individual value is that it
                    is representable as valid SQL by str.format.
        '''

        self.configuration = configuration
        self.table_name    = table_name
        self.columns       = columns
        self.column_names  = [col.name for col in columns]
        self.gen           = gen
        self.parse_output  = parse_output

# list of executables that can be processed
EXECUTABLES = {'gufi_query'       : ExecInfo(gufi_query.CONFIGURATION,
                                             gufi_query.TABLE_NAME,
                                             gufi_query.COLUMNS,
                                             gufi_query.get_numbers,
                                             gufi_query.parse_output),

               'gufi_trace2index' : ExecInfo(gufi_trace2index.CONFIGURATION,
                                             gufi_trace2index.TABLE_NAME,
                                             gufi_trace2index.COLUMNS,
                                             gufi_trace2index.get_numbers,
                                             gufi_trace2index.parse_output)
               # add more executables here
              }

DEFAULT = EXECUTABLES.keys()[0]

# Convert the input argument into an ExecInfo
# fails if not found in the EXECUTABLES dictionary
# (to replicate choices failure in argparse)
def executable_type(key):
    return EXECUTABLES[key]

def add_choices(parser, need_exec):
    parser.add_argument('executable',
                        type=executable_type, default=DEFAULT,
                        help='Known Executables')

    if need_exec:
        parser.add_argument('--executable-path', default=DEFAULT,
                            help='Location of the executable')
