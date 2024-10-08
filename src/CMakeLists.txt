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



cmake_minimum_required(VERSION 3.5.0)

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
  addqueryfuncs.c
  bf.c
  compress.c
  dbutils.c
  debug.c
  descend.c
  external.c
  histogram.c
  print.c
  swap.c
  template_db.c
  trace.c
  trie.c
  utils.c
  xattrs.c
)
add_library(GUFI STATIC ${GUFI_SOURCES})
add_dependencies(GUFI install_dependencies)
set_source_files_properties(bf.c PROPERTIES COMPILE_FLAGS -DGUFI_VERSION="${VERSION_STRING}")

#If the GPFS library exists, build the gpfs scan tool
if (GPFS_LIB AND GPFS_BUILD)
  include_directories("/usr/lpp/mmfs/include")
  add_executable(gpfs-scan-tool gpfs-scan-tool.c)
  target_link_libraries(gpfs-scan-tool ${GPFS_LIB} ${COMMON_LIBRARIES})
endif()

function(build_and_install_one LINK_GUFI TARGET DEFAULT_ALLOCATOR)
  # create the target for building the executable
  add_executable(${TARGET} ${ARGN})

  # link GUFI libraries if needed
  if (LINK_GUFI)
    target_link_libraries(${TARGET} ${COMMON_LIBRARIES})
    add_dependencies(${TARGET} GUFI)
  else()
    add_dependencies(${TARGET} install_dependencies)
  endif()

  # each executable links to its own allocator

  # if the provided allocator is available, set it as the target's default allocator
  if (DEFAULT_ALLOCATOR)
    if (DEFAULT_ALLOCATOR IN_LIST ALLOCATORS)
      set("DEFAULT_${TARGET}_ALLOCATOR" "${DEFAULT_ALLOCATOR}")
    endif()
  endif()

  # if no default allocator was set for this target, set it to the first available allocator
  if (NOT DEFAULT_${TARGET}_ALLOCATOR)
    list(GET ALLOCATORS 0 "DEFAULT_${TARGET}_ALLOCATOR")
  endif()

  set("${TARGET}_ALLOCATOR" "${DEFAULT_${TARGET}_ALLOCATOR}" CACHE STRING "Allocator used by ${TARGET}")
  set_property(CACHE "${TARGET}_ALLOCATOR" PROPERTY STRINGS ${ALLOCATORS})

  # link the allocator
  target_link_libraries(${TARGET} ${${${TARGET}_ALLOCATOR}})

  # install the executable
  install(TARGETS ${TARGET} DESTINATION "${BIN}" COMPONENT Server)
endfunction()

# This function builds C executables using the provided file names
# (SOURCE is an optional argument, so it does not show up in the
# arguments list).
#
# PATH is the installation directory.
#
# There will be duplicate targets if two files share the same name.
#
# ex:
#     build_and_install(TRUE file1.c file2.cpp)
#        -> compiles file1.c   into file1
#        -> compiles file2.cpp into file2
#        -> installs file1 and file2 into bin
#
function(build_and_install LINK_GUFI)
  list(GET ALLOCATORS 0 DEFAULT_ALLOCATOR)
  foreach(SOURCE ${ARGN})
    # get the filename only (no path or extension)
    string(REGEX REPLACE "\\.[^.]*$" "" TARGET ${SOURCE})
    build_and_install_one("${LINK_GUFI}" "${TARGET}" "${DEFAULT_ALLOCATOR}" "${SOURCE}")
  endforeach()
endfunction()

# build and install the binaries that need to link with GUFI
build_and_install_one(TRUE bfwreaddirplus2db    mimalloc bfwreaddirplus2db.c)
build_and_install_one(TRUE dfw                  mimalloc dfw.c)
build_and_install_one(TRUE gufi_dir2index       mimalloc gufi_dir2index.c)
build_and_install_one(TRUE gufi_dir2trace       mimalloc gufi_dir2trace.c)
build_and_install_one(TRUE gufi_index2dir       mimalloc gufi_index2dir.c)
build_and_install_one(TRUE gufi_rollup          mimalloc gufi_rollup.c)
build_and_install_one(TRUE gufi_sqlite3         mimalloc gufi_sqlite3.c) # simple wrapper around sqlite3 to enable GUFI modifications
build_and_install_one(TRUE gufi_trace2dir       mimalloc gufi_trace2dir.c)
build_and_install_one(TRUE gufi_trace2index     jemalloc gufi_trace2index.c)
build_and_install_one(TRUE gufi_treesummary     mimalloc gufi_treesummary.c)
build_and_install_one(TRUE gufi_treesummary_all mimalloc gufi_treesummary_all.c)
build_and_install_one(TRUE gufi_unrollup        mimalloc gufi_unrollup.c)
build_and_install_one(TRUE parallel_cpr         mimalloc parallel_cpr.c)
build_and_install_one(TRUE parallel_rmr         mimalloc parallel_rmr.c)
build_and_install_one(TRUE querydbs             mimalloc querydbs.c)

# this is the gufi_stat binary that operates on indexes directly
build_and_install_one(TRUE gufi_stat_bin        mimalloc gufi_stat.c)

# gufi_query is split up into multiple files
add_library(gufi_query_lib OBJECT
  gufi_query/PoolArgs.c
  gufi_query/aggregate.c
  gufi_query/external.c
  gufi_query/gqw.c
  gufi_query/handle_sql.c
  gufi_query/process_queries.c
  gufi_query/processdir.c
  gufi_query/query.c
)

add_dependencies(gufi_query_lib GUFI)

build_and_install_one(TRUE gufi_query mimalloc
  gufi_query/main.c
  $<TARGET_OBJECTS:gufi_query_lib>
)

# build custom SQLite virtual tables
add_library(gufi_vt MODULE
  gufi_vt.c

  # have to recompile with -fPIC
  SinglyLinkedList.c
  addqueryfuncs.c
  histogram.c
  trie.c
  utils.c
)
set_target_properties(gufi_vt PROPERTIES PREFIX "")
if(APPLE)
  set(EXT "dylib")
elseif (CYGWIN)
  set(EXT "dll.a")
else ()
  set(EXT "so")
endif()
target_link_libraries(gufi_vt "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}")
add_dependencies(gufi_vt install_dependencies)
install(TARGETS gufi_vt DESTINATION ${LIB} COMPONENT Server)

# build binaries that do not need to link with GUFI
set(MISC_SOURCES tsmtime2epoch.c tsmepoch2time.c)
build_and_install(FALSE ${MISC_SOURCES})

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
