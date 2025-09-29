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



# regression test specific files
set(CORE
  generatetree.sh
  os_specific.sh
  setup.sh
)

# support scripts
set(AUX
  hexlify.py
  sbatch.fake
  ssh.fake
)

foreach(NAME ${CORE} ${AUX})
  configure_file("${NAME}.in" "${NAME}" @ONLY)
endforeach()

set(BINARIES
  gufi_dir2index
  gufi_dir2trace
  # gufi_find_outliers
  gufi_index2dir
  gufi_plugin
  gufi_query
  gufi_sqlite3
  gufi_stat_bin
  gufi_trace2dir
  gufi_trace2index
  gufi_treesummary
  gufi_treesummary_all
  querydbs

  gufi_vt
  run_vt
)

set(PYTHON
  gufi_du
  gufi_find
  gufi_getfattr
  gufi_ls
  gufi_stat
  gufi_stats
)

if (CLIENT)
  list(APPEND PYTHON
    gufi_query.py
  )
endif()

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
  deploy
  external_databases
  gufi_distributed
  gufi_jail
  longitudinal_snapshot
  parallel_cpr
  parallel_find
  parallel_rmr
  treediff
)

if (COMPOPT EQUAL 0)
  list(APPEND OTHERS
    bash_completion
  )
endif()

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

# replacement main for python scripts
configure_file(gufi_tool.py.in gufi_tool.py @ONLY)

# gufi_vt.common is always required by gufi_vt test
configure_file(gufi_vt.common gufi_vt.common @ONLY)

# check for SQLAlchemy and add test if found
execute_process(COMMAND python3 -c "import sqlalchemy"
  RESULT_VARIABLE SQLALCHEMY_RC
  OUTPUT_QUIET ERROR_QUIET)
if (SQLALCHEMY_RC EQUAL 0)
  message(STATUS "SQLAlchemy found. Adding test.")
  list(APPEND PYTHON
    gufi_vt_sqlalchemy
  )

  # python wrapper tool (not part of scripts)
  configure_file(gufi_vt_sqlalchemy.py.in gufi_vt_sqlalchemy.py @ONLY)

  # check for pugsql and add test if found
  execute_process(COMMAND python3 -c "import pugsql"
    RESULT_VARIABLE PUGSQL_RC
    OUTPUT_QUIET ERROR_QUIET)
  if (PUGSQL_RC EQUAL 0)
    message(STATUS "pugsql found. Adding test.")
    list(APPEND PYTHON
      gufi_vt_pugsql
    )

    # python wrapper tool (not part of scripts)
    configure_file(gufi_vt_pugsql.py.in gufi_vt_pugsql.py @ONLY)
  else()
    message(STATUS "pugsql not found.")
  endif()
else()
  message(STATUS "SQLAlchemy not found.")
endif()

# .sh and .expected
set(SH_AND_EXPECTED
  ${BINARIES}
  ${PYTHON}
  ${EXAMPLES}
  ${OTHERS}
)

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
  if (APPLE OR DARWIN)
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
