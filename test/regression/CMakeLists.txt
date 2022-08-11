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

function(check_test_dependencies var exec)
  find_program(${var} ${exec})        # REQUIRED option was not added until 3.18
  if (${var})
    message(STATUS "Found ${exec}: ${${var}}")
  else ()
    # try alternative
    if (${ARGC} GREATER 2)
      message(WARNING "${exec} not found. Trying alternative: ${ARGV2}")
      find_program(${var} ${ARGV2})   # REQUIRED option was not added until 3.18
    endif()

    if (${var})
      message(STATUS "Found alternative to ${exec}: ${${var}}")
    else ()
      message(FATAL_ERROR "${var} not found. Cannot build tests.")
    endif()
  endif()
endfunction()

check_test_dependencies(SED  sed)
check_test_dependencies(DIFF colordiff diff)
check_test_dependencies(TEE  tee)

if(APPLE)
  check_test_dependencies(XATTR xattr)
else()
  check_test_dependencies(SETFATTR setfattr)
endif()

# regression test specific files
set(CORE
  generatetree.sh
  os_setfattr.sh
  setup.sh

  common.py
  version_control.py
)

# .sh and .expected
set(SH_AND_EXPECTED
  completions
  gufi_dir2index
  gufi_dir2trace
  gufi_trace2index
  gufi_query
  querydbs
)

# .sh, .py, and .expected
set(SH_PY_AND_EXPECTED
  gufi_find
  gufi_getfattr
  gufi_ls
  gufi_stats
)

if (CMAKE_CXX_COMPILER)
  list(APPEND SH_AND_EXPECTED
    verifytrace
    verifytraceintree
  )
endif()

if (NOT APPLE)
  list(APPEND SH_AND_EXPECTED
    gufi_stat)
endif()

foreach(NAME ${CORE})
  configure_file("${NAME}.in" "${NAME}" @ONLY)
endforeach()

foreach(NAME ${SH_AND_EXPECTED})
  configure_file("${NAME}.sh.in"    "${NAME}.sh"       @ONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

  add_test(NAME "${NAME}" COMMAND "${NAME}.sh")
  set_tests_properties("${NAME}" PROPERTIES LABELS regression)
endforeach()

foreach(NAME ${SH_PY_AND_EXPECTED})
  configure_file("${NAME}.sh.in"    "${NAME}.sh"       @ONLY)
  configure_file("${NAME}.py.in"    "${NAME}.py"       @ONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

  add_test(NAME "${NAME}" COMMAND "${NAME}.sh")
  set_tests_properties("${NAME}" PROPERTIES LABELS regression)
endforeach()

find_program(SUDO sudo)
if (SUDO)
  set(ENABLE_SUDO_TESTS Off CACHE BOOL "Whether or not to enable tests that need sudo")
  if (ENABLE_SUDO_TESTS)
    # .sh and .expected
    set(NEED_SUDO
      rollup
      unrollup
      xattrs
      )

    foreach(NAME ${NEED_SUDO})
      configure_file("${NAME}.sh.in"    "${NAME}.sh"       @ONLY)
      configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

      add_test(NAME "${NAME}" COMMAND ${SUDO} "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.sh")
      set_tests_properties("${NAME}" PROPERTIES LABELS sudo)
    endforeach()
  endif()
else()
  message(STATUS "sudo executable not found. Not running tests requiring sudo.")
endif()
