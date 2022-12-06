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



from configparser import ConfigParser, ExtendedInterpolation
from matplotlib import pyplot as plt # should be able to get rid of this

from performance_pkg import common

# converter functions for input read from ConfigParser
def pos_int(value):
    val = int(value)
    if val < 1:
        raise ValueError('Positive integer got a non-positive number: {0}'.format(val))
    return val

def pos_float(value):
    val = float(value)
    if val <= 0:
        raise ValueError('Positive float got a non-positive number: {0}'.format(val))
    return val

def to_list(value, convert=str):
    return [convert(item.strip()) for item in value.split(',')]

def str_list(value):
    return to_list(value)

def pos_float_list(value):
    values = to_list(value, float)
    for val in values:
        if val <= 0:
            raise ValueError('Positive float list got a non-positive number: {0}'.format(val))
    return values

# config sections and keys
RAW_DATA = 'raw_data'                   # section
RAW_DATA_COMMITS = 'commits'            # string list
RAW_DATA_COLUMNS = 'columns'            # string list

OUTPUT = 'output'                       # section
OUTPUT_PATH = 'path'                    # string
OUTPUT_GRAPH_TITLE = 'graph_title'      # string
OUTPUT_DIMENSIONS = 'graph_dimensions'  # postive float pair

LINES = 'lines'                         # section
LINES_COLORS = 'colors'                 # string list
LINES_TYPES = 'types'                   # string list
LINES_MARKERS = 'markers'               # string list

AXES = 'axes'                           # section
AXES_HASH_LEN = 'hash_len'              # integer
AXES_Y_LABEL = 'y_label'                # string
AXES_Y_MIN = 'y_min'                    # float
AXES_Y_MAX = 'y_max'                    # float

ANNOTATIONS = 'annotations'             # section
ANNOTATIONS_SHOW = 'show'               # bool
ANNOTATIONS_PRECISION = 'precision'     # positive integer
ANNOTATIONS_X_OFFSET = 'x_offset'       # float
ANNOTATIONS_Y_OFFSET = 'y_offset'       # float
ANNOTATIONS_TEXT_COLORS = 'text_colors' # string list

ERROR_BAR = 'error_bar'                 # section
ERROR_BAR_SHOW = 'show'                 # bool
ERROR_BAR_PRECISION = 'precision'       # positive integer
ERROR_BAR_MIN_MAX = 'min_max'           # bool
ERROR_BAR_COLORS = 'colors'             # string list
ERROR_BAR_CAP_SIZE = 'cap_size'         # positive float

DEFAULTS = {
    RAW_DATA : {
        RAW_DATA_COMMITS : [str_list, []],
        RAW_DATA_COLUMNS : [str_list, []],
    },

    OUTPUT : {
        OUTPUT_PATH        : [str, ''],
        OUTPUT_GRAPH_TITLE : [str, ''],
        OUTPUT_DIMENSIONS  : [pos_float_list, [0.0, 0.0]],
    },

    LINES : {
        LINES_COLORS  : [str_list, []],
        LINES_TYPES   : [str_list, []],
        LINES_MARKERS : [str_list, []],
    },

    AXES: {
        AXES_HASH_LEN : [int, 0],
        AXES_Y_LABEL  : [str, 'Y Axis'],
        AXES_Y_MIN    : [float, None],
        AXES_Y_MAX    : [float, None],
    },

    ANNOTATIONS : {
        ANNOTATIONS_SHOW        : [bool, False],
        ANNOTATIONS_PRECISION   : [pos_int, 3],
        ANNOTATIONS_X_OFFSET    : [float, 5],
        ANNOTATIONS_Y_OFFSET    : [float, 5],
        ANNOTATIONS_TEXT_COLORS : [str_list, []],
    },

    ERROR_BAR : {
        ERROR_BAR_SHOW      : [bool, False],
        ERROR_BAR_PRECISION : [pos_int, 3],
        ERROR_BAR_MIN_MAX   : [bool, False],
        ERROR_BAR_COLORS    : [str_list, []],
        ERROR_BAR_CAP_SIZE  : [pos_float, 10],
    },
}

def read_value(parser, section, key):
    convert, default = DEFAULTS[section][key]
    try:
        value = convert(parser.get(section, key))
    except: # pylint: disable=bare-except
        value = default
    return value

def read_bool(parser, section, key):
    _, default = DEFAULTS[section][key]
    try:
        value = parser.getboolean(section, key)
    except: # pylint: disable=bare-except
        value = default
    return value

def expand_git_identifiers(identifiers):
    commits = []
    for ish in identifiers:
        # try expanding identifier using git
        if '..' in ish:
            expanded = common.run_get_stdout(['git', 'rev-list', ish])
        else:
            expanded = common.run_get_stdout(['git', 'rev-parse', ish])

        # failure to expand still returns the original ish as output
        commits += expanded.split('\n')[-2::-1]  # remove last empty line and reverse list

    return commits

def process(filename):
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(filename)

    git_identifiers = read_value(parser, RAW_DATA, RAW_DATA_COMMITS)
    raw_data = {
        RAW_DATA_COMMITS : expand_git_identifiers(git_identifiers),
        RAW_DATA_COLUMNS : read_value(parser, RAW_DATA, RAW_DATA_COLUMNS),
    }

    # how many columns to pull/lines to plot
    column_count = len(raw_data[RAW_DATA_COLUMNS])

    dimensions = read_value(parser, OUTPUT, OUTPUT_DIMENSIONS)[:2]
    output = {
        OUTPUT_PATH : read_value(parser, OUTPUT, OUTPUT_PATH),
        OUTPUT_GRAPH_TITLE : read_value(parser, OUTPUT, OUTPUT_GRAPH_TITLE),
        OUTPUT_DIMENSIONS : tuple(dimensions),
    }

    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    lines = {
        LINES_COLORS : read_value(parser, LINES, LINES_COLORS) + list(colors * column_count),
        LINES_TYPES : read_value(parser, LINES, LINES_TYPES) + (['solid'] * column_count),
        LINES_MARKERS : read_value(parser, LINES, LINES_MARKERS) + (['o'] * column_count),
    }

    axes = {
        AXES_HASH_LEN : read_value(parser, AXES, AXES_HASH_LEN),
        AXES_Y_LABEL : read_value(parser, AXES, AXES_Y_LABEL),
        AXES_Y_MIN : read_value(parser, AXES, AXES_Y_MIN),
        AXES_Y_MAX : read_value(parser, AXES, AXES_Y_MAX),
    }

    annotations = {
        ANNOTATIONS_SHOW : read_bool(parser, ANNOTATIONS, ANNOTATIONS_SHOW),
        ANNOTATIONS_PRECISION : read_value(parser, ANNOTATIONS, ANNOTATIONS_PRECISION),
        ANNOTATIONS_X_OFFSET : read_value(parser, ANNOTATIONS, ANNOTATIONS_X_OFFSET),
        ANNOTATIONS_Y_OFFSET : read_value(parser, ANNOTATIONS, ANNOTATIONS_Y_OFFSET),
        ANNOTATIONS_TEXT_COLORS : read_value(parser, ANNOTATIONS, ANNOTATIONS_TEXT_COLORS) + (['black'] * column_count),
    }

    error_bar = {
        ERROR_BAR_SHOW : read_bool(parser, ERROR_BAR, ERROR_BAR_SHOW),
        ERROR_BAR_PRECISION : read_value(parser, ERROR_BAR, ERROR_BAR_PRECISION),
        ERROR_BAR_MIN_MAX : read_bool(parser, ERROR_BAR, ERROR_BAR_MIN_MAX),
        ERROR_BAR_COLORS : read_value(parser, ERROR_BAR, ERROR_BAR_COLORS) + (['red'] * column_count),
        ERROR_BAR_CAP_SIZE : read_value(parser, ERROR_BAR, ERROR_BAR_CAP_SIZE),
    }

    return raw_data, output, lines, axes, annotations, error_bar
