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

# build the simple source tree generator
add_executable(gendir gendir.c)
target_link_libraries(gendir ${COMMON_LIBRARIES})
add_dependencies(gendir GUFI)

# potentially useful C++ executables
if (CMAKE_CXX_COMPILER)
  # a more complex index generator
  add_executable(make_testindex make_testindex.cpp)
  target_link_libraries(make_testindex ${COMMON_LIBRARIES})
  add_dependencies(make_testindex GUFI)

  add_executable(verifytrace verifytrace.cpp)

  add_executable(verifytraceintree verifytraceintree.cpp)
  target_link_libraries(verifytraceintree ${COMMON_LIBRARIES})
  add_dependencies(verifytraceintree install_dependencies)
endif()

# potentially useful scripts
set(USEFUL
  canned_queries.sh
  gentrace.py
  hashes.py
  trace_anonymizer.py
  )

foreach(file ${USEFUL})
    configure_file("${file}" "${file}" @ONLY)
endforeach()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_subdirectory(debug)
endif()

add_subdirectory(CI)
if (GIT_FOUND)
  # git -C <path>
  set(GIT_PERFORMANCE_VERSION "1.8.5")

  if (CMAKE_VERSION GREATER "3.14")
    set(GIT_VERSION "${GIT_VERSION_STRING}")
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} --version
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE GIT_VERSION_STRING
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REGEX MATCH "[0-9]+\.[0-9]+(\.[0-9]+)?" GIT_VERSION "${GIT_VERSION_STRING}")
  endif()
  message(STATUS "git version: ${GIT_VERSION}")

  # VERSION_GREATER_EQUAL was introduced in CMake 3.7
  if (GIT_VERSION VERSION_LESS GIT_PERFORMANCE_VERSION)
    message(STATUS "Not adding performance history framework (need git 1.8.5+)")
  else()
    if (CMAKE_BUILD_TYPE STREQUAL "Debug" AND PRINT_CUMULATIVE_TIMES)
      message(STATUS "Adding performance history framework")
      add_subdirectory(performance)
    else()
      message(STATUS "Not adding performance history framework (need PRINT_CUMULATIVE_TIMES enabled)")
    endif()
  endif()
endif()
