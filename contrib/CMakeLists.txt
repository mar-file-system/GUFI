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

# make benchmark
#    clean up old benchmark directory and create a new one
#    generate a tree
#    index the tree with bfwi
#    query the tree with gufi_find
#    delete the benchmark directory
set(BENCHMARK Off CACHE BOOL "Whether or not to enable the benchmark target")
if (BENCHMARK)
  message(WARNING "Make sure IndexRoot in the GUFI config also points to BENCHMARK_ROOT")
  set(BENCHMARK_ROOT "/tmp/gufi-benchmark" CACHE PATH "Where to place benchmark trees.")

  function(check_int VAR)
    string(REGEX MATCH "^[0-9]+$" PARSED "${${VAR}}")
    if (PARSED STREQUAL "")
      message(FATAL_ERROR "Invalid ${VAR} value: \"${${VAR}}\"")
    endif()
  endfunction()

  set(BENCHMARK_DIRS  "5" CACHE STRING "Number of directories per level in the benchmark tree")
  check_int(BENCHMARK_DIRS)

  set(BENCHMARK_FILES "5" CACHE STRING "Number of files per directory in the benchmark tree")
  check_int(BENCHMARK_FILES)

  set(BENCHMARK_DEPTH "5" CACHE STRING "Number of levels in the benchmark tree")
  check_int(BENCHMARK_DEPTH)

  include(ProcessorCount)
  ProcessorCount(N)
  set(BENCHMARK_THREADS "${N}" CACHE STRING "Number of levels in the benchmark tree")
  check_int(BENCHMARK_DEPTH)
  if (NOT "${BENCHMARK_THREADS}" GREATER "0")
    message(FATAL_ERROR "Need at least one thread to run executables")
  endif()

  set(BENCHMARK_TREE "${BENCHMARK_ROOT}/tree")
  set(BENCHMARK_GUFI_TREE "${BENCHMARK_ROOT}/gufi_tree")
  add_custom_target(benchmark_init COMMAND $<TARGET_FILE:parallel_rmr> -n "${BENCHMARK_THREADS}" "${BENCHMARK_ROOT}"
                                   COMMAND ${CMAKE_COMMAND} -E make_directory "${BENCHMARK_ROOT}"
                    DEPENDS parallel_rmr )
  add_custom_target(benchmark_generate_dir COMMAND $<TARGET_FILE:gendir> "${BENCHMARK_TREE}" "${BENCHMARK_DIRS}" "${BENCHMARK_FILES}" "${BENCHMARK_DEPTH}" "${BENCHMARK_THREADS}"
                    DEPENDS benchmark_init gendir)
  add_custom_target(benchmark_dir2index COMMAND $<TARGET_FILE:gufi_dir2index> -n "${BENCHMARK_THREADS}" "${BENCHMARK_TREE}" "${BENCHMARK_GUFI_TREE}"
                    DEPENDS benchmark_generate_dir gufi_dir2index)
  add_custom_target(benchmark_query COMMAND ${CMAKE_BINARY_DIR}/scripts/gufi_find > /dev/null
                    DEPENDS benchmark_dir2index gufi_query ${CMAKE_BINARY_DIR}/scripts/gufi_find)
  add_custom_target(benchmark_fin COMMAND $<TARGET_FILE:parallel_rmr> -n "${BENCHMARK_THREADS}" "${BENCHMARK_ROOT}"
                    DEPENDS benchmark_query parallel_rmr)
  add_custom_target(benchmark DEPENDS benchmark_fin)
endif()

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
