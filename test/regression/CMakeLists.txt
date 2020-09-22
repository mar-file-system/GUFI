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

# regression test specific files
set(CORE
  generatetree
  setup.sh
  common.py
)

# .sh and .expected
set(SH_AND_EXPECTED
  completions
  gufi_dir2index
  gufi_dir2trace
  gufi_trace2index
  gufi_query
  querydbn
)

# .sh, .py, and .expected
set(SH_PY_AND_EXPECTED
  gufi_find
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

# .sh and .expected
# tests are not added since sudo is required
set(SUDO
  rollup
  unrollup
)

foreach(NAME ${CORE})
  configure_file(${NAME} ${NAME} COPYONLY)
endforeach()

foreach(NAME ${SH_AND_EXPECTED})
  configure_file("${NAME}.sh"       "${NAME}.sh"       COPYONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

  add_test(NAME "${NAME}" COMMAND "${NAME}.sh")
  set_tests_properties("${NAME}" PROPERTIES LABELS regression)
endforeach()

foreach(NAME ${SH_PY_AND_EXPECTED})
  configure_file("${NAME}.sh"       "${NAME}.sh"       COPYONLY)
  configure_file("${NAME}.py"       "${NAME}.py"       COPYONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)

  add_test(NAME "${NAME}" COMMAND "${NAME}.sh")
  set_tests_properties("${NAME}" PROPERTIES LABELS regression)
endforeach()

foreach(NAME ${SUDO})
  configure_file("${NAME}.sh"       "${NAME}.sh"       COPYONLY)
  configure_file("${NAME}.expected" "${NAME}.expected" COPYONLY)
endforeach()
