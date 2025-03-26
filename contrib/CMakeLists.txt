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



set(CONTRIB_SOURCES
  gendir.c                # simple source tree generator
  treediff.c              # walk two trees and print the names that don't match
)

# potentially useful C++ executables
if (CMAKE_CXX_COMPILER)
  list(APPEND CONTRIB_SOURCES
    make_testindex.cpp    # a more complex index generator
    verifytraceintree.cpp
  )

  add_executable(verifytrace verifytrace.cpp)
endif()

# build C/C++ sources that depend on libGUFI
foreach(SOURCE ${CONTRIB_SOURCES})
  # NAME_WE removes the directory and the longest extension
  get_filename_component(TARGET "${SOURCE}" NAME_WE)
  add_executable("${TARGET}" "${SOURCE}")
  target_link_libraries("${TARGET}" ${COMMON_LIBRARIES})
endforeach()

set(CHECKSTYLE)

# add 'make shellcheck'
find_program(SHELLCHECK shellcheck)
if (SHELLCHECK)
  message(STATUS "Found shellcheck: ${SHELLCHECK}")
  configure_file(shellcheck.sh shellcheck.sh @ONLY)
  add_custom_target(shellcheck
    COMMAND "${CMAKE_CURRENT_BINARY_DIR}/shellcheck.sh" "${CMAKE_BINARY_DIR}")
  list(APPEND CHECKSTYLE shellcheck)
else()
  message(STATUS "shellcheck not found")
endif()

# add 'make pylint'
find_program(PYLINT pylint)
if (PYLINT)
  message(STATUS "Found pylint: ${PYLINT}")
  configure_file(pylint.sh pylint.sh @ONLY)
  add_custom_target(pylint
    COMMAND "${CMAKE_CURRENT_BINARY_DIR}/pylint.sh" "${CMAKE_BINARY_DIR}")
  list(APPEND CHECKSTYLE pylint)
else()
  message(STATUS "pylint not found")
endif()

# add 'make checkstyle'
if (CHECKSTYLE)
  message(STATUS "checkstyle will run: ${CHECKSTYLE}")
  add_custom_target(checkstyle
    DEPENDS ${CHECKSTYLE})
else()
  message(STATUS "checkstyle disabled")
endif()

# potentially useful scripts
set(USEFUL
  canned_queries.sh
  gentrace.py
  hashes.py
  longitudinal_snapshot.py
  trace_anonymizer.py
)

foreach(file ${USEFUL})
  configure_file("${file}" "${file}" @ONLY)
endforeach()

add_subdirectory(CI)

# See if lustre-devel is installed by checking if the lustreapi.h header is in a standard include path.
find_path(LUSTRE_PLUGIN "lustre/lustreapi.h")
# If so, build the lustre example plugin:
if (LUSTRE_PLUGIN)
  add_library(lustre_plugin SHARED lustre_plugin.c)
  target_link_libraries(lustre_plugin lustreapi)
endif()
