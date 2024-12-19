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

# Don't allow in-source build
get_filename_component(CMAKE_SOURCE_DIR "${CMAKE_SOURCE_DIR}" REALPATH)
get_filename_component(CMAKE_BINARY_DIR "${CMAKE_BINARY_DIR}" REALPATH)
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR "Refusing to build in source directory \"${CMAKE_SOURCE_DIR}\"")
endif()

# Don't allow in-source or in-build installs
# CMAKE_INSTALL_PREFIX is not set by CMake until later on, so set it here so that the real path can be used
set(CMAKE_INSTALL_PREFIX "/usr/local" CACHE PATH "Install path prefix, prepended onto install directories.")
get_filename_component(CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}" REALPATH)
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_INSTALL_PREFIX)
  message(FATAL_ERROR "Refusing to install into source directory \"${CMAKE_SOURCE_DIR}\"")
endif()
if (CMAKE_BINARY_DIR STREQUAL CMAKE_INSTALL_PREFIX)
  message(FATAL_ERROR "Refusing to install into build directory \"${CMAKE_BINARY_DIR}\"")
endif()

project(GUFI VERSION 0.6.5 LANGUAGES C)

# print version info
find_package(Git)                     # not required
set(VERSION_STRING "${GUFI_VERSION}") # default to CMake project version
if ((GIT_FOUND) AND (EXISTS "${CMAKE_SOURCE_DIR}/.git"))
  # get latest tag
  execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=0
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_TAG
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # get commit of latest tag
  execute_process(COMMAND ${GIT_EXECUTABLE} rev-list -n 1 "${GIT_TAG}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_TAG_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # git HEAD commit hash
  execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_HEAD_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # check if anything has changed
  execute_process(COMMAND ${GIT_EXECUTABLE} status --porcelain --untracked-files=no
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_HEAD_MODIFIED
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(LENGTH "${GIT_HEAD_MODIFIED}" GIT_HEAD_MODIFIED_LEN)
  set(GIT_HEAD_DIRTY "")
  if (NOT GIT_HEAD_MODIFIED_LEN EQUAL "0")
    set(GIT_HEAD_DIRTY "-dirty")
  endif()

  # if on a tag, show the tag name; otherwise show the commit
  if (GIT_TAG_COMMIT STREQUAL GIT_HEAD_COMMIT)
    set(VERSION_STRING "${GIT_TAG}")
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --short "${GIT_HEAD_COMMIT}"
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE GIT_HEAD_COMMIT_SHORT
      OUTPUT_STRIP_TRAILING_WHITESPACE)

    set(VERSION_STRING "${GIT_HEAD_COMMIT_SHORT}")
  endif()

  # add "-dirty" if changes are present
  set(VERSION_STRING "${VERSION_STRING}${GIT_HEAD_DIRTY}")
endif()
message(STATUS "GUFI ${VERSION_STRING}")

# Require C99
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Need _XOPEN_SRC
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_XOPEN_SRC")

# Define the DEBUG macro when building in debug mode
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG -Wall")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  option(GPROF "Compile with -pg" Off)
  if (GPROF)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -pg")
  endif()

  option(GCOV "Enable Code Coverage" Off)
  if (GCOV)
    set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}   --coverage")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage")
  endif()
endif()

# Only enable C++ if support is found
include(CheckLanguage)
check_language(CXX)
if (CMAKE_CXX_COMPILER)
  enable_language(CXX)

  # GCC prior to 4.9.3 doesn't have full standard support
  if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.3")
    set(CMAKE_CXX_COMPILER false)
    message(STATUS "Require GNU C++ version 4.9.3 or greater: Not compiling C++ sources")
  elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "3.9")
    set(CMAKE_CXX_COMPILER false)
    message(STATUS "Require Clang version 3.9 or greater: not compiling C++ sources")
  else()
    # Require C++11
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)

    # Need _XOPEN_SRC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_XOPEN_SRC")

    # Define the DEBUG macro when building in debug mode
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall")

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
      if (GPROF)
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -pg")
      endif()
    endif()
  endif()
else()
  set(CMAKE_CXX_COMPILER false)
  message(STATUS "Not compiling C++ sources")
endif()

# Python is required
if (CMAKE_VERSION VERSION_GREATER "3.12.0")
  find_package(Python REQUIRED COMPONENTS Interpreter)
else()
  find_program(Python NAMES python python3 python2) # REQUIRED option was not added until 3.18
  if (NOT Python)
    message(FATAL_ERROR "Python not found")
  endif()
  execute_process(COMMAND ${Python} --version
    OUTPUT_VARIABLE Python_VERSION_STDOUT
    ERROR_VARIABLE Python_VERSION_STDERR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE)
  string(REGEX REPLACE "^Python " "" Python_VERSION "${Python_VERSION_STDOUT}${Python_VERSION_STDERR}")
  string(REPLACE "." ";" Python_VERSION_LIST ${Python_VERSION})
  list(GET Python_VERSION_LIST 0 Python_VERSION_MAJOR)
  message(STATUS "Found Python: ${Python} (found version \"${Python_VERSION}\") found components: Interpreter")
endif()

set(PYTHON_INTERPRETER "python${Python_VERSION_MAJOR}" CACHE STRING "Python Interpreter")
message(STATUS "Using Python Interpreter: ${PYTHON_INTERPRETER}")

# require find_program to return executables with -r+x permissions
if (POLICY CMP0109)
  cmake_policy(SET CMP0109 NEW)
endif()

# find a required executable dependency
function(find_exec_dependency var)
  # REQUIRED option was not added until 3.18
  find_program(${var} NAMES ${ARGN})
  if (${var})
    message(STATUS "Found ${var}: ${${var}}")
  else()
    message(FATAL_ERROR "Required program ${var} not found.")
  endif()
endfunction()

# from coreutils, so should always be available, and if
# found, all other coreutils commands are also available
find_exec_dependency(TRUNCATE truncate)

# not from coreutils, but still very common
find_exec_dependency(AWK   awk gawk)
find_exec_dependency(DIFF  colordiff diff)
find_exec_dependency(FIND  find)
find_exec_dependency(GREP  grep)
find_exec_dependency(PATCH patch)
find_exec_dependency(SED   sed)
if(APPLE)
  find_exec_dependency(XATTR xattr)
else()
  find_exec_dependency(SETFATTR setfattr)
endif()

# Need the PkgConfig module for pkg_search_module
find_package(PkgConfig REQUIRED)

# Tell CMake to search the contrib/cmake directory cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/contrib/cmake ${CMAKE_MODULE_PATH})

# Detect and handle xattr.h platform specifics
find_package(XATTR REQUIRED)
message(STATUS "xattr headers located in ${XATTR_INCLUDEDIR}")
set(CPACK_RPM_PACKAGE_REQUIRES "libattr")
if (XATTR_ATTR_XATTR_HEADER)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_ATTR_XATTR_H")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCONFIG_ATTR_XATTR_H")
endif()
if (XATTR_SYS_XATTR_HEADER)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_SYS_XATTR_H")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCONFIG_SYS_XATTR_H")
endif()
if (APPLE)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_DARWIN_XATTRS")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCONFIG_DARWIN_XATTRS")
elseif (CYGWIN)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_CYGWIN_XATTRS")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCONFIG_CYGWIN_XATTRS")
else()
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_GNU_XATTRS")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCONFIG_GNU_XATTRS")
endif()

set(BIN "bin" CACHE PATH "Directory under \${CMAKE_INSTALL_PREFIX} to install binaries")
set(LIB "lib" CACHE PATH "Directory under \${CMAKE_INSTALL_PREFIX} to install binaries")

# Need pthread
find_package(Threads REQUIRED)

# Need PCRE2 for linking only
pkg_search_module(PCRE2 REQUIRED libpcre2-8)

set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}")

# Download and build all non-system dependencies
set(DEP_DOWNLOAD_PREFIX    "${CMAKE_SOURCE_DIR}/contrib/deps" CACHE PATH   "Location of dependency sources")
set(DEP_BUILD_PREFIX       "${CMAKE_BINARY_DIR}/builds"       CACHE PATH   "Location of dependency builds")
set(DEP_BUILD_THREADS      "1"                                CACHE STRING "Number of build threads")
set(DEP_INSTALL_PREFIX     "${CMAKE_BINARY_DIR}/deps"         CACHE PATH   "Location of dependency binaries")
set(CLIENT                 OFF                                CACHE BOOL   "Whether or not to build and install client")
set(DEP_PATCH_SQLITE3_OPEN OFF                                CACHE BOOL   "Whether or not to patch SQLite3 open")
set(DEP_USE_JEMALLOC       ON                                 CACHE BOOL   "Whether or not to build and link with jemalloc")
set(DEP_OPTIONS --threads "${DEP_BUILD_THREADS}")
if (CMAKE_CXX_COMPILER)
  set(DEP_OPTIONS ${DEP_OPTIONS} --cxx)
endif()
if (DEP_PATCH_SQLITE3_OPEN)
  set(DEP_OPTIONS ${DEP_OPTIONS} --patch-sqlite3-open)
endif()
if (DEP_USE_JEMALLOC)
  set(DEP_OPTIONS ${DEP_OPTIONS} --jemalloc)
endif()
add_custom_target(install_dependencies ALL COMMAND ${CMAKE_SOURCE_DIR}/contrib/deps/install_deps.sh ${DEP_OPTIONS} "${DEP_DOWNLOAD_PREFIX}" "${DEP_BUILD_PREFIX}" "${DEP_INSTALL_PREFIX}" "${CMAKE_VERSION}" "${CMAKE_SYSTEM_NAME}")

# Find zlib
pkg_search_module(ZLIB zlib)
if (ZLIB_FOUND)
  message(STATUS "ZLIB headers located in ${ZLIB_INCLUDEDIR}")
  message(STATUS "ZLIB libraries located in ${ZLIB_LIBDIR}")
  set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}, zlib")
  add_definitions(-DHAVE_ZLIB=1)
else()
  message(STATUS "ZLIB not found.")
endif()

# search for FUSE on Linux, or OSXFUSE on OSX
if (UNIX AND NOT APPLE AND NOT CYGWIN)
  pkg_search_module(FUSE fuse)
  if (FUSE_FOUND)
    message(STATUS "FUSE headers located in ${FUSE_INCLUDEDIR}")
    message(STATUS "FUSE libraries located in ${FUSE_LIBDIR}")

    set(MY_FUSE_FOUND YES)
    set(MY_FUSE_CFLAGS ${FUSE_CFLAGS})
    set(MY_FUSE_LDFLAGS ${FUSE_LDFLAGS})
    set(MY_FUSE_VERSION ${FUSE_VERSION})
  endif()
elseif (APPLE)
  pkg_search_module(OSXFUSE osxfuse)
  if (OSXFUSE_FOUND)
    message(STATUS "OSXFUSE headers located in ${OSXFUSE_INCLUDEDIR}")
    message(STATUS "OSXFUSE libraries located in ${OSXFUSE_LIBDIR}")

    set(MY_FUSE_FOUND YES)
    set(MY_FUSE_CFLAGS ${OSXFUSE_CFLAGS})
    set(MY_FUSE_LDFLAGS ${OSXFUSE_LDFLAGS})
    set(MY_FUSE_VERSION ${OSXFUSE_VERSION})
  endif()
endif()

if (MY_FUSE_FOUND)
  option(FUSE_BUILD "Build FUSE GUFI tools" ON)
  if (FUSE_BUILD)
    set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}, fuse-libs")
  else()
    message(STATUS "FUSE found but not building bffuse and bfresultfuse")
  endif()
else()
  message(STATUS "FUSE not found. Not building bffuse and bfresultfuse")
endif()

# Find GPFS
find_library(GPFS_LIB gpfs PATH "/usr/lpp/mmfs/lib")
if (GPFS_LIB)
  message(STATUS "GPFS libraries found; building gpfs-scan-tool")
  option(GPFS_BUILD "Build GPFS GUFI tools" ON)
  if (GPFS_BUILD)
    set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}, gpfs.base")
  else()
    message(STATUS "GPFS libraries found but not building gpfs-scan-tool")
  endif()
else()
  message(STATUS "GPFS libraries not found; not building gpfs-scan-tool")
endif()

# Find DB2
pkg_search_module(DB2 db2)
if (DB2_FOUND)
  message(STATUS "DB2 headers located in ${DB2_INCLUDEDIR}")
  message(STATUS "DB2 libraries located in ${DB2_LIBDIR}")
  option(DB2_BUILD "Build DB2 GUFI tools" ON)
  if (DB2_BUILD)
    set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}, db2")
  else()
    message(STATUS "DB2 found but not building DB2 tools")
  endif()
else()
  message(STATUS "DB2 not found")
endif()

# everyone includes (-I) these paths, whether or not they need them
set(COMMON_INCLUDES
  ${CMAKE_SOURCE_DIR}/include
  ${XATTR_INCLUDEDIR}
  ${DEP_INSTALL_PREFIX}/sqlite3/include
  ${DEP_INSTALL_PREFIX}/sqlite3-pcre)

include_directories(${COMMON_INCLUDES})

# set of libraries needed by the final binaries
set(COMMON_LIBRARIES
  GUFI
  ${DEP_INSTALL_PREFIX}/sqlite3-pcre/libsqlite3-pcre.a
  ${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.a
  ${PCRE2_LDFLAGS}
  Threads::Threads
  m
)

if (ZLIB_FOUND)
  list(APPEND COMMON_LIBRARIES
    z
  )
endif()

if (DEP_USE_JEMALLOC)
  list(APPEND COMMON_LIBRARIES
    ${DEP_INSTALL_PREFIX}/jemalloc/lib/libjemalloc.a
  )
endif()

list(APPEND COMMON_LIBRARIES
  dl
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  # ##########################################
  # Debug prints
  # ##########################################

  # Cumulative times shown at end of run
  option(PRINT_CUMULATIVE_TIMES "Sum the durations of events across all threads and print them at the end" Off)
  if (PRINT_CUMULATIVE_TIMES)
    add_definitions(-DCUMULATIVE_TIMES=1)
  endif()

  option(PRINT_PER_THREAD_STATS "Print individual events of each thread" Off)
  if (PRINT_PER_THREAD_STATS)
    add_definitions(-DPER_THREAD_STATS=1)
  endif()

  option(PRINT_QPTPOOL_QUEUE_SIZE "Print the un-pulled work queue size of each thread" Off)
  if (PRINT_QPTPOOL_QUEUE_SIZE)
    add_definitions(-DQPTPOOL_QUEUE_SIZE=1)
  endif()
endif()

# enable "make test"
enable_testing()

# configuration files
add_subdirectory(config)

# build the core executables
add_subdirectory(src)

# build the core scripts
add_subdirectory(scripts)

# build documentation
add_subdirectory(docs)

# recurse down into the test subdirectory
add_subdirectory(test)

# build contrib executables
add_subdirectory(contrib)

# copy examples
add_subdirectory(examples)

#
# Generate Packages
#

# Generate RPMs
find_program(RPMBUILD rpmbuild)
if (RPMBUILD)
  message(STATUS "Found rpmbuild: ${RPMBUILD}")

  # get the architecture of this machine
  # assumes that we are not cross compiling
  execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)

  # These variables have to be set before include(CPack)
  set(CPACK_PACKAGE_VERSION_MAJOR ${GUFI_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${GUFI_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${GUFI_VERSION_PATCH})

  set(CPACK_GENERATOR "RPM")
  set(CPACK_RPM_COMPONENT_INSTALL On)
  set(CPACK_RPM_FILE_NAME "gufi-${GUFI_VERSION}.${ARCH}.rpm")
  set(CPACK_RPM_PACKAGE_SUMMARY "Grand Unified File Index")
  set(CPACK_RPM_PACKAGE_NAME "GUFI")
  set(CPACK_RPM_PACKAGE_VERSION "${GUFI_VERSION}")
  set(CPACK_RPM_PACKAGE_RELEASE 1)
  set(CPACK_RPM_PACKAGE_LICENSE "BSD/LANL")
  set(CPACK_RPM_PACKAGE_VENDOR "LANL")
  set(CPACK_RPM_PACKAGE_URL "https://github.com/mar-file-system/GUFI")
  set(CPACK_RPM_PACKAGE_REQUIRES "${CPACK_RPM_PACKAGE_REQUIRES}, ${PYTHON_INTERPRETER}, coreutils")
  set(CPACK_RPM_PACKAGE_RELOCATABLE On)
  set(CPACK_RPM_PACKAGE_ARCHITECTURE "${ARCH}")
  set(CPACK_RPM_Server_FILE_NAME "gufi-server-${GUFI_VERSION}.${ARCH}.rpm")
  set(CPACK_RPM_Server_PACKAGE_SUMMARY "GUFI Server RPM")

  if (CLIENT)
    set(CPACK_RPM_Client_FILE_NAME "gufi-client-${GUFI_VERSION}.${ARCH}.rpm")
    # python libraries are installed through pip, and won't be found by rpm
    set(CPACK_RPM_Client_PACKAGE_REQUIRES "${PYTHON_INTERPRETER}, openssh, ${PYTHON_INTERPRETER}-pip")
    set(CPACK_RPM_Client_PACKAGE_SUMMARY "GUFI Client RPM")
    set(CPACK_RPM_Client_PRE_INSTALL_SCRIPT_FILE "${CMAKE_BINARY_DIR}/contrib/deps/client/pip_install.sh")
  endif()

  # Generate source RPM
  set(CPACK_SOURCE_PACKAGE_FILE_NAME "gufi-${GUFI_VERSION}")
  set(CPACK_SOURCE_IGNORE_FILES "${CMAKE_BINARY_DIR}/" "${CMAKE_SOURCE_DIR}/.git/" "${CMAKE_SOURCE_DIR}/.github/" "${CMAKE_SOURCE_DIR}/.gitignore")

  include(CPack)
else()
  message(STATUS "rpmbuild not found. 'make package' will not be available.")
endif()
