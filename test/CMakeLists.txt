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



# allow for the test working directory to be moved
set(TEST_WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}" CACHE PATH "Directory to run tests in")
execute_process(COMMAND "${CMAKE_COMMAND}" -E make_directory "${TEST_WORKING_DIRECTORY}"
    COMMAND_ERROR_IS_FATAL ANY)

# add regression tests
add_subdirectory(regression)

# make sure unit tests work first
add_subdirectory(unit)

# copy test scripts into the test directory within the build directory
# list these explicitly to prevent random garbage from getting in
foreach(TEST
    bfwiflat2gufitest
    dfw2gufitest
    gufitest.py
    robinhoodin
    runbffuse
    runbfq
    runbfqforfuse
    runbfti
    runbfwi
    rundfw
    rungenuidgidsummaryavoidentriesscan
    rungroupfilespacehog
    rungroupfilespacehogusesummary
    runlistschemadb
    runlisttablesdb
    runoldbigfiles
    runquerydbs
    runuidgidsummary
    runuidgidummary
    runuserfilespacehog
    runuserfilespacehogusesummary
    runtests
  )
  # copy the script into the build directory for easy access
  configure_file("${TEST}" "${TEST}" @ONLY)
endforeach()

set(TESTTAR "${CMAKE_CURRENT_SOURCE_DIR}/testdir.tar")
set(TESTDIR "${TEST_WORKING_DIRECTORY}/testdir")
set(TESTDST "${TEST_WORKING_DIRECTORY}/gary")

add_test(
  NAME gary
  COMMAND "${CMAKE_COMMAND}" -E env "PATH=${DEP_INSTALL_PREFIX}/sqlite3/bin:$ENV{PATH}" "${CMAKE_CURRENT_BINARY_DIR}/runtests" "${TESTTAR}" "${TESTDIR}" "${TESTDST}"
  WORKING_DIRECTORY "${TEST_WORKING_DIRECTORY}")
set_tests_properties(gary PROPERTIES LABELS "manual")

# add_test(NAME gufitest COMMAND gufitest.py all)
# set_tests_properties(gufitest PROPERTIES LABELS manual)

foreach(type index query)
  add_library("test_${type}ing_plugin" SHARED "test_${type}ing_plugin.c")
  add_dependencies("test_${type}ing_plugin" install_dependencies)
endforeach()

if (APPLE OR DARWIN)
  # Apple needs to be told to explicitly not try to resolve symbols at compile time when
  # building the plugin. (They will be resolved dynamically when the plugin is dlopen()ed).
  foreach(type index query)
    target_link_libraries("test_${type}ing_plugin" -Wl,-undefined -Wl,dynamic_lookup)
  endforeach()
elseif(CYGWIN)
  # Cygwin needs to use an "import library" (reference: https://cygwin.com/cygwin-ug-net/dll.html)
  # to get the symbol definitions it uses from gufi_dir2index.
  target_link_libraries("test_indexing_plugin" "${CMAKE_BINARY_DIR}/src/libgufi_dir2index.dll.a")
  target_link_libraries("test_querying_plugin" "${CMAKE_BINARY_DIR}/src/libgufi_query.dll.a")
else()
  foreach(type index query)
    target_link_libraries("test_${type}ing_plugin" "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}")
  endforeach()
endif()
