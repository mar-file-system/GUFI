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

# make sure unit tests work first
add_subdirectory(googletest)

# simple create known tree and query test
add_test(NAME verifyknowntree COMMAND verifyknowntree)

# copy test scripts into the test directory within the build directory
# list these explicitly to prevent random garbage from getting in
foreach(SCRIPT
    bfwiflat2gufitest
    dfw2gufitest
    generatetree
    gitest.py
    gufitest.py
    robinhoodin
    runbffuse
    runbfmi
    runbfq
    runbfqforfuse
    runbfti
    runbfwi
    runbfwreaddirplus2db
    runbfwreaddirplus2db.orig
    rundfw
    rungenuidgidsummaryavoidentriesscan
    rungroupfilespacehog
    rungroupfilespacehogusesummary
    runlistschemadb
    runlisttablesdb
    runoldbigfiles
    runquerydb
    runquerydbn
    runuidgidsummary
    runuidgidummary
    runuserfilespacehog
    runuserfilespacehogusesummary
    testdir.tar
    verifyknowntree)
  # copy the scropt into the build directory for easy access
  configure_file(${SCRIPT} ${SCRIPT} COPYONLY)
endforeach()

# create an empty directory and extract testdir.tar into it for runtests
set(TESTTAR  "${CMAKE_CURRENT_SOURCE_DIR}/testdir.tar")
set(TESTDIR  "${CMAKE_CURRENT_BINARY_DIR}/testdir")
set(TESTDST  "${TESTDIR}.gufi")
set(TESTGUFI "${TESTDST}/${TESTDIR}")

add_custom_target(cleanup_tree ALL COMMAND rm -rf "${TESTDIR}" "${TESTGUFI}")
add_custom_target(mkdir ALL COMMAND mkdir -p "${TESTDIR}"
                  DEPENDS cleanup_tree)
add_custom_target(untar_tree ALL COMMAND tar -xf "${TESTTAR}" -C "${TESTDIR}"
                  DEPENDS mkdir)

# run individual tests
# bfwi must run first
add_test(NAME bfwi                            COMMAND runbfwi                        ${TESTDIR} ${TESTDST} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME bfti                            COMMAND runbfti                        ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME bfq                             COMMAND runbfq                         ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME bfwreaddirplus2db               COMMAND runbfwreaddirplus2db           ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME querydb                         COMMAND runquerydb                     ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME querydbn                        COMMAND runquerydbn                    ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME dfw                             COMMAND rundfw                         ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# other tests
add_test(NAME listschemadb                    COMMAND runlistschemadb                ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME listtablesdb                    COMMAND runlisttablesdb                ${TESTGUFI}           WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME groupfilespacehogusesummary     COMMAND rungroupfilespacehogusesummary ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME userfilespacehogusesummary      COMMAND runuserfilespacehogusesummary  ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME groupfilespacehog               COMMAND rungroupfilespacehog           ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME userfilespacehog                COMMAND runuserfilespacehog            ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_test(NAME oldbigfiles                     COMMAND runoldbigfiles                 ${TESTGUFI} 2         WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# add_test(NAME gufitest COMMAND gufitest.py all)
