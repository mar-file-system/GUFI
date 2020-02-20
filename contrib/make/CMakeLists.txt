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

#
# Generate a tarball         (make gary)
# build and test the tarball (make test_gary)
#

# List of entries from the root
# directory that will be tarred
set(ROOT
  CMakeLists.txt
  INSTALL
  LICENSE.txt
  Makefile
  README.md
  contrib
  docs
  examples
  include
  scripts
  src
  test
)

# Set the output name
set(TARBALL_NAME "${CMAKE_BINARY_DIR}/gufi.tar.gz")

add_custom_target(gary COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/root       ${CMAKE_SOURCE_DIR}/Makefile
                       COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/src        ${CMAKE_SOURCE_DIR}/src/Makefile
                       COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/scripts    ${CMAKE_SOURCE_DIR}/scripts/Makefile
                       COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/test       ${CMAKE_SOURCE_DIR}/test/Makefile
                       COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/googletest ${CMAKE_SOURCE_DIR}/test/googletest/Makefile
                       COMMAND ${CMAKE_COMMAND} -E tar "czf" ${TARBALL_NAME} ${ROOT} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                       COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/Makefile
                       COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/src/Makefile
                       COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/scripts/Makefile
                       COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/test/Makefile
                       COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_SOURCE_DIR}/test/googletest/Makefile
                       DEPENDS ${CMAKE_SOURCE_DIR}/test/googletest)

# Test the tarball
set(TARBALL_TEST_DIR "${CMAKE_BINARY_DIR}/tarball")

add_custom_target(make_tarball_dir COMMAND ${CMAKE_COMMAND} -E remove_directory ${TARBALL_TEST_DIR}
                                   COMMAND ${CMAKE_COMMAND} -E make_directory ${TARBALL_TEST_DIR})

add_custom_target(build_gary COMMAND ${CMAKE_COMMAND} -E tar "xzf" ${TARBALL_NAME}
                             COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} DEP_INSTALL_PREFIX=${DEP_INSTALL_PREFIX} make DEBUG=1
                             COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} DEP_INSTALL_PREFIX=${DEP_INSTALL_PREFIX} make test
                             WORKING_DIRECTORY ${TARBALL_TEST_DIR}
                             DEPENDS gary make_tarball_dir)

add_custom_target(test_gary COMMAND ${CMAKE_COMMAND} -E remove_directory ${TARBALL_TEST_DIR}
                            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
                            DEPENDS build_gary)
