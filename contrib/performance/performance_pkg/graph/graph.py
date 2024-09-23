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
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    for x_val, y_val in zip(x_vals, y_vals):
        plt.annotate('{0:.{1}f}'.format(y_val, precision),
                     (x_val, y_val),
                     color=color,
                     textcoords="offset points",
                     xytext=(x_offset, y_offset))

def generate(conf, const_x, ys):
    '''
    y_vals, line_names, eb_mins, eb_maxs, annot_mins, annot_maxes
    should all be lists of lists with the same dimensions
    '''

    # pylint: disable=too-many-locals,too-many-branches

    # pad and unpack config
    output, lines, axes, annotations, error_bar = pad_config(conf)

    # plot data
    plt.figure(figsize=tuple(output[config.OUTPUT_DIMENSIONS][:2]))
    plt.title(output[config.OUTPUT_GRAPH_TITLE])
    plt.xlabel('Commit') # fixed
    if axes[config.AXES_X_LABEL_ROTATION] is not None:
        plt.tick_params(axis='x', labelrotation=axes[config.AXES_X_LABEL_ROTATION])
    if axes[config.AXES_X_LABEL_SIZE] is not None:
        plt.tick_params(axis='x', labelsize=axes[config.AXES_X_LABEL_SIZE])
    plt.ylabel(axes[config.AXES_Y_LABEL])

    line_configs = zip(ys.items(),
                       lines[config.LINES_COLORS],
                       lines[config.LINES_TYPES],
                       lines[config.LINES_MARKERS],
                       annotations[config.ANNOTATIONS_TEXT_COLORS],
                       error_bar[config.ERROR_BAR_COLORS])

    for line, color, style, marker, acolor, ecolor in line_configs:
        label       = line[0]                           # name of this line
        y_vals      = line[1]                           # collection of values for the current line
        y_vals_main = y_vals[axes[config.AXES_Y_STAT]]  # main points to plot for this line

        # annotate main points
        if axes[config.AXES_ANNOTATE]:
            add_annotations(const_x, y_vals_main,
                            annotations[config.ANNOTATIONS_PRECISION],
                            acolor,
                            annotations[config.ANNOTATIONS_X_OFFSET],
                            annotations[config.ANNOTATIONS_Y_OFFSET])

        yerr = None

        # set up bottom error bar
        if error_bar[config.ERROR_BAR_BOTTOM] is not None:
            y_vals_bottom = y_vals[error_bar[config.ERROR_BAR_BOTTOM]]
            if error_bar[config.ERROR_BAR_ANNOTATE]:
                add_annotations(const_x, y_vals_bottom,
                                annotations[config.ANNOTATIONS_PRECISION],
                                acolor,
                                annotations[config.ANNOTATIONS_X_OFFSET],
                                annotations[config.ANNOTATIONS_Y_OFFSET])

            if yerr is None:
                yerr = [y_vals_main, y_vals_main]

            yerr[0] = [main - bottom for main, bottom in zip(y_vals_main, y_vals_bottom)]

        # set up top error bar
        if error_bar[config.ERROR_BAR_TOP] is not None:
            y_vals_top = y_vals[error_bar[config.ERROR_BAR_TOP]]
            if error_bar[config.ERROR_BAR_ANNOTATE]:
                add_annotations(const_x, y_vals_top,
                                annotations[config.ANNOTATIONS_PRECISION],
                                acolor,
                                annotations[config.ANNOTATIONS_X_OFFSET],
                                annotations[config.ANNOTATIONS_Y_OFFSET])

            if yerr is None:
                yerr = [y_vals_main, y_vals_main]

            yerr[1] = [top - main for main, top in zip(y_vals_main, y_vals_top)]

        plotline, caps, _ = plt.errorbar(const_x, y_vals_main, label=label,
                                         yerr=yerr,
                                         barsabove=True,
                                         lolims=(error_bar[config.ERROR_BAR_BOTTOM] is None),
                                         uplims=(error_bar[config.ERROR_BAR_TOP] is None),
                                         ecolor=ecolor,
                                         capsize=error_bar[config.ERROR_BAR_CAP_SIZE])

        # set error bar caps
        # setting in the errorbar call doesn't seem to work
        for cap in caps:
            cap.set_marker('_')

        plotline.set_color(color)
        plotline.set_linestyle(style)
        plotline.set_marker(marker)

    if axes[config.AXES_Y_MIN] is not None:
        plt.ylim(bottom=axes[config.AXES_Y_MIN])

    if axes[config.AXES_Y_MAX] is not None:
        plt.ylim(top=axes[config.AXES_Y_MAX])

    plt.legend()
    plt.savefig(output[config.OUTPUT_PATH],
                bbox_inches='tight') # remove extra whitespace around border
