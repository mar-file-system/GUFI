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



cmake_minimum_required(VERSION 3.0.0)

if (CUMULATIVE_TIMES OR PER_THREAD_STATS OR SUBDIRECTORY_COUNTS OR QPTPOOL_QUEUE_SIZE)
  message(WARNING "At least one of the debug print variables have been enabled, so regression tests have been disabled")
else()
  message(STATUS "Regression tests enabled")

  # copy test scripts into the test directory within the build directory
  # list these explicitly to prevent random garbage from getting in
  set(REGRESSION_TEST_FILES
    gufi_dir2index.expected
    gufi_dir2index.sh

    gufi_dir2trace.expected
    gufi_dir2trace.sh

    gufi_trace2index.expected
    gufi_trace2index.sh

    gufi_query.expected
    gufi_query.sh

    querydb.expected
    querydb.sh

    querydbn.expected
    querydbn.sh

    generatetree
    setup.sh
    common.py

    gufi_find.expected
    gufi_find.py
    gufi_find.sh

    gufi_ls.expected
    gufi_ls.py
    gufi_ls.sh

    gufi_stats.expected
    gufi_stats.py
    gufi_stats.sh

    completions.expected
    completions.sh
    )

  if (NOT APPLE)
    list(APPEND REGRESSION_TEST_FILES
      gufi_stat.expected
      gufi_stat.sh)
  endif()

  if (CMAKE_CXX_COMPILER)
    list(APPEND REGRESSION_TEST_FILES
      verifytrace.expected
      verifytrace.sh

      verifytraceintree.expected
      verifytraceintree.sh)
  endif()

  foreach(FILE ${REGRESSION_TEST_FILES})
    configure_file(${FILE} ${FILE} COPYONLY)
    get_filename_component(EXT ${FILE} EXT)
    if ("${EXT}" STREQUAL ".sh")
      if ("${FILE}" STREQUAL "setup.sh")
        continue()
      endif()

      get_filename_component(TEST_NAME ${FILE} NAME_WE)
      add_test(NAME ${TEST_NAME} COMMAND ${FILE})
      set_tests_properties(${TEST_NAME} PROPERTIES LABELS regression)
    endif()
  endforeach()
endif()
