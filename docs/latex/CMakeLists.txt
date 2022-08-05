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



cmake_minimum_required(VERSION 2.8)
SET(LATEX_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/pdfs)

INCLUDE(UseLATEX)

set(image_path "images")

file(RELATIVE_PATH license ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/LICENSE.txt)
set(common_inputs
  GUFIText.sty
  ${license}
  sections/license.tex
  )

set(gufi_developer_inputs
    sections/introduction.tex
    sections/requirements.tex
    sections/design.tex
    sections/schema.tex
    sections/indexing.tex
    sections/gufi_dir2index.tex
    sections/tracefile.tex
    sections/gufi_dir2trace.tex
    sections/gufi_trace2index.tex
    sections/postindexing.tex
    sections/rollup.tex
    sections/bfti.tex
    sections/xattrs.tex
    sections/querying.tex
    sections/gufi_query.tex
    sections/querydbs.tex
    sections/addqueryfuncs.tex
    sections/implementation.tex
    sections/qptp.tex
    sections/bottomup.tex
    sections/printing.tex
    sections/optimizations.tex
    )

set(gufi_administrator_inputs
    sections/introduction.tex
    sections/dependencies.tex
    sections/download.tex
    sections/build.tex
    sections/install.tex
    sections/schema.tex
    sections/indexing.tex
    sections/gufi_dir2index.tex
    sections/tracefile.tex
    sections/gufi_dir2trace.tex
    sections/gufi_trace2index.tex
    sections/postindexing.tex
    sections/rollup.tex
    sections/bfti.tex
    sections/xattrs.tex
    sections/querying.tex
    sections/gufi_query.tex
    sections/querydbs.tex
    sections/addqueryfuncs.tex
    sections/deploy.tex
    )

set(gufi_user_inputs
    sections/introduction.tex
    sections/user_env.tex
    sections/gufi_find.tex
    sections/gufi_ls.tex
    sections/gufi_stats.tex
    sections/gufi_stat.tex
    )

ADD_LATEX_DOCUMENT(
    gufi_developer.tex
    INPUTS ${common_inputs} ${gufi_developer_inputs}
    IMAGE_DIRS ${image_path}
    )

ADD_LATEX_DOCUMENT(
    gufi_administrator.tex
    INPUTS ${common_inputs} ${gufi_administrator_inputs}
    IMAGE_DIRS ${image_path}
    )

ADD_LATEX_DOCUMENT(
    gufi_user.tex
    INPUTS ${common_inputs} ${gufi_user_inputs}
    IMAGE_DIRS ${image_path}
    )
