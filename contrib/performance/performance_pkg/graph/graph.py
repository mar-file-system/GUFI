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



from matplotlib import pyplot as plt

from performance_pkg.graph import config

def pad_config(conf):
    '''
    pad certain configuration settings with extra data to make sure
    graphing doesn't run out of values
    '''

    # how many columns to pull/lines to plot
    column_count = len(conf[config.RAW_DATA][config.RAW_DATA_COLUMNS])

    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    conf[config.LINES][config.LINES_COLORS]  += list(colors * column_count)
    conf[config.LINES][config.LINES_TYPES]   += ['solid'] * column_count
    conf[config.LINES][config.LINES_MARKERS] += ['o'] * column_count

    conf[config.ANNOTATIONS][config.ANNOTATIONS_TEXT_COLORS] += ['black'] * column_count

    conf[config.ERROR_BAR][config.ERROR_BAR_COLORS] += ['red'] * column_count

    return [
        # raw data is not needed
        conf[config.OUTPUT],
        conf[config.LINES],
        conf[config.AXES],
        conf[config.ANNOTATIONS],
        conf[config.ERROR_BAR],
    ]

def add_annotations(x_vals, y_vals,
                    precision,
                    color,
                    x_offset, y_offset):
    # pylint: disable=too-many-arguments
    for x_val, y_val in zip(x_vals, y_vals):
        plt.annotate('{0:.{1}f}'.format(y_val, precision),
                     (x_val, y_val),
                     color=color,
                     textcoords="offset points",
                     xytext=(x_offset, y_offset))

def generate(conf, const_x, y_vals, line_names,    # pylint: disable=too-many-arguments
             eb_mins, eb_maxs,
             annot_mins, annot_maxs):
    '''
    y_vals, line_names, eb_mins, eb_maxs, annot_mins, annot_maxes
    should all be lists of lists with the same dimensions
    '''

    # pylint: disable=too-many-locals

    # pad and unpack config
    output, lines, axes, annotations, error_bar = pad_config(conf)

    # plot data
    plt.figure(figsize=output[config.OUTPUT_DIMENSIONS])
    plt.title(output[config.OUTPUT_GRAPH_TITLE])
    plt.xlabel('Commit') # fixed
    plt.ylabel(axes[config.AXES_Y_LABEL])
    if axes[config.AXES_Y_MIN] is not None and axes[config.AXES_Y_MAX] is not None:
        plt.ylim(axes[config.AXES_Y_MIN], axes[config.AXES_Y_MAX])

    line_configs = zip(y_vals, line_names,
                       lines[config.LINES_COLORS],
                       lines[config.LINES_TYPES],
                       lines[config.LINES_MARKERS],
                       annotations[config.ANNOTATIONS_TEXT_COLORS],
                       annot_mins, annot_maxs,
                       error_bar[config.ERROR_BAR_COLORS],
                       eb_mins, eb_maxs)

    for (y_curr, label, color, style, marker, acolor,
         annot_min, annot_max, ecolor, eb_min, eb_max) in line_configs:
        yerr = None
        if annotations[config.ANNOTATIONS_SHOW]:
            add_annotations(const_x, y_curr,
                            annotations[config.ANNOTATIONS_PRECISION],
                            acolor,
                            annotations[config.ANNOTATIONS_X_OFFSET],
                            annotations[config.ANNOTATIONS_Y_OFFSET])

        if error_bar[config.ERROR_BAR_SHOW]:
            yerr = (eb_min, eb_max)

            if annotations[config.ANNOTATIONS_SHOW] and error_bar[config.ERROR_BAR_MIN_MAX]:
                add_annotations(const_x, annot_min,
                                error_bar[config.ERROR_BAR_PRECISION],
                                acolor,
                                annotations[config.ANNOTATIONS_X_OFFSET],
                                annotations[config.ANNOTATIONS_Y_OFFSET])
                add_annotations(const_x, annot_max,
                                error_bar[config.ERROR_BAR_PRECISION],
                                acolor,
                                annotations[config.ANNOTATIONS_X_OFFSET],
                                annotations[config.ANNOTATIONS_Y_OFFSET])

        plotline, _, _ = plt.errorbar(const_x, y_curr, label=label,
                                      yerr=yerr,
                                      ecolor=ecolor,
                                      capsize=error_bar[config.ERROR_BAR_CAP_SIZE])

        plotline.set_color(color)
        plotline.set_linestyle(style)
        plotline.set_marker(marker)

    plt.legend()
    plt.savefig(output[config.OUTPUT_PATH],
                bbox_inches='tight') # remove extra whitespace around border
