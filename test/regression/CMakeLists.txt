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



cmake_minimum_required(VERSION 3.1.0)

# regression test specific files
set(CORE
  generatetree.sh
  os_setfattr.sh
  setup.sh
)

foreach(NAME ${CORE})
  configure_file("${NAME}.in" "${NAME}" @ONLY)
endforeach()

set(BINARIES
  gufi_dir2index
  gufi_dir2trace
  gufi_trace2index
  gufi_treesummary
  gufi_treesummary_all
  gufi_query
  gufi_stat_bin
  querydbs
)

set(PYTHON
  gufi_find
  gufi_getfattr
  gufi_ls
  gufi_stat
  gufi_stats
)

set(EXAMPLES
  deluidgidsummaryrecs
  generategidsummary
  generateuidsummary
  gengidsummaryavoidentriesscan
  genuidsummaryavoidentriesscan
  groupfilespacehog
  groupfilespacehogusesummary
  oldbigfiles
  userfilespacehog
  userfilespacehogusesummary
)

set(OTHERS
  completions
)

if (ZLIB_FOUND)
  list(APPEND OTHERS
    compression
  )
endif()

if (CMAKE_CXX_COMPILER)
  list(APPEND OTHERS
    verifytrace
    verifytraceintree
  )
endif()

# .sh and .expected
set(SH_AND_EXPECTED
  ${BINARIES}
  ${PYTHON}
  ${EXAMPLES}
  ${OTHERS}
)

configure_file(gufi_tool.py.in gufi_tool.py)

foreach(NAME ${SH_AND_EXPECTED})
  configure_file("${NAME}.sh.in"    "${NAME}.sh"       @ONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

  add_test(NAME "${NAME}" COMMAND "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.sh"
    WORKING_DIRECTORY "${TEST_WORKING_DIRECTORY}")
endforeach()

foreach(NAME ${BINARIES})
  set_tests_properties("${NAME}" PROPERTIES LABELS "regression;binary")
endforeach()

foreach(NAME ${EXAMPLES})
  set_tests_properties("${NAME}" PROPERTIES LABELS "regression;examples")
endforeach()

foreach(NAME ${OTHERS})
  set_tests_properties("${NAME}" PROPERTIES LABELS "regression")
endforeach()

foreach(NAME ${PYTHON})
  set_tests_properties("${NAME}" PROPERTIES LABELS "regression;python")
endforeach()

find_program(SUDO sudo)
if (SUDO)
  if (APPLE)
    set(SUDO sudo)
  endif()

  set(ENABLE_SUDO_TESTS Off CACHE BOOL "Whether or not to enable tests that need sudo")
  if (ENABLE_SUDO_TESTS)
    configure_file("rollup_tree.sh.in" "rollup_tree.sh" @ONLY)

    set(SUDO_BINARIES
      gufi_rollup
      gufi_unrollup
    )

    set(SUDO_OTHERS
      xattrs
    )

    # .sh and .expected
    set(NEED_SUDO
      ${SUDO_BINARIES}
      ${SUDO_OTHERS}
    )

    foreach(NAME ${NEED_SUDO})
      configure_file("${NAME}.sh.in"    "${NAME}.sh"       @ONLY)
      configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

      add_test(NAME "${NAME}" COMMAND ${SUDO} "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.sh"
        WORKING_DIRECTORY "${TEST_WORKING_DIRECTORY}")
    endforeach()

    foreach(NAME ${SUDO_BINARIES})
      set_tests_properties("${NAME}" PROPERTIES LABELS "regression;sudo;binary")
    endforeach()

    foreach(NAME ${SUDO_OTHERS})
      set_tests_properties("${NAME}" PROPERTIES LABELS "regression;sudo")
    endforeach()

  endif()
else()
  message(STATUS "sudo executable not found. Not running tests requiring sudo.")
endif()
