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

include(CheckSymbolExists)
check_symbol_exists(localtime_r "time.h" LOCALTIME_R)
if (LOCALTIME_R)
  add_definitions(-DLOCALTIME_R)
endif()

# ##########################################
# flags that can be used to modify code
# behavior at compile time for some binaries.
#
# All of these should be left at their
# default values when running the ctest.
#
# All binaries should be able to run
# even if all of the values are changed.
#

# opendb can be turned off
# addqueryfuncs fails silently if not turned off
# sqlite3_exec fails with error message if not turned off
option(RUN_OPENDB "Whether or not to enable opening databases" ON)
if (RUN_OPENDB)
  add_definitions(-DOPENDB=1)
endif()

# addqueryfuncs can be turned off
option(RUN_ADDQUERYFUNCS "Add custom functions to opened databases" ON)
if (RUN_ADDQUERYFUNCS)
  add_definitions(-DADDQUERYFUNCS=1)
endif()

# sqlite3_exec can be turned off
option(RUN_SQL_EXEC "Call sqlite3_exec" ON)
if (RUN_SQL_EXEC)
  add_definitions(-DSQL_EXEC=1)
endif()
# ##########################################

# create the GUFI library, which contains all of the common source files
set(GUFI_SOURCES
  BottomUp.c
  OutputBuffers.c
  QueuePerThreadPool.c
  SinglyLinkedList.c
  bf.c
  compress.c
  dbutils.c
  debug.c
  external.c
  histogram.c
  print.c
  template_db.c
  trace.c
  trie.c
  utils.c
  xattrs.c
)
add_library(GUFI STATIC ${GUFI_SOURCES})
add_dependencies(GUFI install_dependencies)

#If the GPFS library exists, build the gpfs scan tool
if (GPFS_LIB AND GPFS_BUILD)
  include_directories("/usr/lpp/mmfs/include")
  add_executable(gpfs-scan-tool gpfs-scan-tool.c)
  target_link_libraries(gpfs-scan-tool ${GPFS_LIB} ${COMMON_LIBRARIES})
endif()

# This function builds C executables using the provided file names
# (SOURCE is an optional argument, so it does not show up in the
# arguments list).
#
# PATH is the installation directory.
#
# There will be duplicate targets if two files share the same name.
#
# ex:
#     build_and_install(bin TRUE file1.c file2.cpp)
#        -> compiles file1.c   into file1
#        -> compiles file2.cpp into file2
#        -> installs file1 and file2 into bin
#
function(build_and_install_one PATH LINK_GUFI TARGET)
  # create the target for building the executable
  add_executable(${TARGET} ${ARGN})

  # link GUFI libraries if needed
  if (LINK_GUFI)
    target_link_libraries(${TARGET} ${COMMON_LIBRARIES})
    add_dependencies(${TARGET} GUFI)
  endif()

  # install the executable
  install(TARGETS ${TARGET} DESTINATION ${PATH} COMPONENT Server)
endfunction()

function(build_and_install PATH LINK_GUFI)
  foreach(SOURCE ${ARGN})
    # get the filename only (no path or extension)
    string(REGEX REPLACE "\\.[^.]*$" "" TARGET ${SOURCE})
    build_and_install_one("${PATH}" "${LINK_GUFI}" "${TARGET}" "${SOURCE}")
  endforeach()
endfunction()

# build and install the binaries that need to link with GUFI
set(C_SOURCES
  bfwreaddirplus2db.c
  dfw.c
  gufi_dir2index.c
  gufi_dir2trace.c
  gufi_rollup.c
  gufi_sqlite3.c          # simple wrapper around sqlite3 to enable GUFI modifications
  gufi_trace2index.c
  gufi_treesummary.c
  gufi_treesummary_all.c
  gufi_unrollup.c
  parallel_cpr.c
  parallel_rmr.c
  querydbs.c
)

# build the C executables
build_and_install("${BIN}" TRUE ${C_SOURCES})

# this is the gufi_stat binary that operates on indexes directly
build_and_install_one("${BIN}" TRUE gufi_stat_bin gufi_stat.c)

# gufi_query is split up into multiple files
add_library(gufi_query_lib OBJECT
  gufi_query/PoolArgs.c
  gufi_query/aggregate.c
  gufi_query/external.c
  gufi_query/processdir.c
  gufi_query/process_queries.c
  gufi_query/query.c
  gufi_query/timers.c
  gufi_query/validate_inputs.c
  gufi_query/xattrs.c
)

add_dependencies(gufi_query_lib GUFI)

build_and_install_one(${BIN} TRUE gufi_query
  gufi_query/main.c
  $<TARGET_OBJECTS:gufi_query_lib>
)

# build binaries that do not need to link with GUFI
set(MISC_SOURCES tsmtime2epoch.c tsmepoch2time.c)
build_and_install(${BIN} FALSE ${MISC_SOURCES})

# only build bffuse and bfresultfuse if FUSE is found
if (MY_FUSE_FOUND AND FUSE_BUILD)
  # remove the semicolons that get added
  string(REPLACE ";" " " MY_FUSE_CFLAGS "${MY_FUSE_CFLAGS}")
  string(REPLACE ";" " " MY_FUSE_LDFLAGS "${MY_FUSE_LDFLAGS}")

  # create the bffuse executable
  add_executable(bffuse bffuse.c)
  set_target_properties(bffuse PROPERTIES COMPILE_FLAGS "${MY_FUSE_CFLAGS}")
  target_link_libraries(bffuse ${COMMON_LIBRARIES} ${MY_FUSE_LDFLAGS})

  # create the bfresultfuse executable
  add_executable(bfresultfuse bfresultfuse.c)
  set_target_properties(bfresultfuse PROPERTIES COMPILE_FLAGS "${MY_FUSE_CFLAGS}")
  target_link_libraries(bfresultfuse ${COMMON_LIBRARIES} ${MY_FUSE_LDFLAGS})

  # install the fuse binaries in ${CMAKE_INSTALL_PREFIX}/${BIN}
  install(TARGETS bffuse bfresultfuse DESTINATION ${BIN} COMPONENT Server)
endif()

# only build db2 sources if db2 is found
if (DB2_FOUND AND DB2_BUILD)
  string(REPLACE ";" " " DB2_CFLAGS "${DB2_CFLAGS}")
  string(REPLACE ";" " " DB2_LDFLAGS "${DB2_LDFLAGS}")
endif()
