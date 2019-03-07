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
# -----
# NOTE:
# -----
#
# GUFI uses the C-Thread-Pool library.  The original version, written by
# Johan Hanssen Seferidis, is found at
# https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
# released under the MIT License.  LANS, LLC added functionality to the
# original work.  The original work, plus LANS, LLC added functionality is
# found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
# License.  The MIT License can be found at
# https://opensource.org/licenses/MIT.
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
project(GUFI C)

# Require C99
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# define the DEBUG macro when building in debug mode
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG -Wall")

# only enable C++ if support is found
include(CheckLanguage)
check_language(CXX)
if (CMAKE_CXX_COMPILER)
  enable_language(CXX)

  # enable C++11
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)

  # define the DEBUG macro when building in debug mode
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall")
else()
  message(STATUS "Not compiling C++ sources")
endif()

# need the PkgConfig module for pkg_search_module
find_package(PkgConfig REQUIRED)

# tell CMake to search the contrib/cmake directory cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/contrib/cmake ${CMAKE_MODULE_PATH})

# need xattr.h
find_package(xattr REQUIRED XATTR)
message(STATUS "xattr headers located in ${XATTR_INCLUDEDIR}")

# need uuid
pkg_search_module(UUID REQUIRED uuid)
message(STATUS "UUID headers located in ${UUID_INCLUDEDIR}")
message(STATUS "UUID libraries located in ${UUID_LIBDIR}")

# need pthread
find_package(Threads REQUIRED)

# load the ExternalProject module
include(ExternalProject)

# Allow the user to chose to use HTTPS or SSH
set(GIT_DL_METHOD HTTPS CACHE STRING "Use HTTPS or SSH to download git repositories")
string(TOLOWER "${GIT_DL_METHOD}" GIT_DL_METHOD)

# download and build C-Thread-Pool
set(thpool_PREFIX ${CMAKE_BINARY_DIR}/src)
if ("${GIT_DL_METHOD}" STREQUAL "https")
  ExternalProject_Add(C-Thread-Pool
    GIT_REPOSITORY    https://github.com/mar-file-system/C-Thread-Pool.git
    GIT_TAG           lanl
    GIT_SHALLOW       1
    PREFIX            ${CMAKE_BINARY_DIR}/
    STEP_TARGETS      build
    INSTALL_COMMAND   ""
  )
elseif ("${GIT_DL_METHOD}" STREQUAL "ssh")
  ExternalProject_Add(C-Thread-Pool
    GIT_REPOSITORY    git@github.com:mar-file-system/C-Thread-Pool.git
    GIT_TAG           lanl
    GIT_SHALLOW       1
    PREFIX            ${CMAKE_BINARY_DIR}/
    STEP_TARGETS      build
    INSTALL_COMMAND   ""
  )
endif()

# set the install rpath so the executables that link
# against C-Thread-Pool can find it after being installed
set(CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_RPATH} ${thpool_PREFIX}/lib)

# search for sqlite3 installation
pkg_search_module(SQLITE3 REQUIRED sqlite3)
if (SQLITE3_VERSION VERSION_LESS 3.13)
  message(FATAL_ERROR "GUFI requires sqlite3 3.13+. Got ${SQLITE3_VERSION}")
endif()
include_directories(AFTER SYSTEM ${SQLITE3_INCLUDE_DIRS}) # -Ipath/to/sqlite3/include
link_directories(${SQLITE3_LIBRARY_DIRS})                 # -Lpath/to/sqlite3/lib
message(STATUS "SQLite3 headers located in ${SQLITE3_INCLUDEDIR}")
message(STATUS "SQLite3 libraries located in ${SQLITE3_LIBDIR}")

# download and build sqlite3-pcre
set(pcre_PREFIX     ${CMAKE_BINARY_DIR}/src/sqlite3-pcre)
if ("${GIT_DL_METHOD}" STREQUAL "https")
  ExternalProject_Add(sqlite3-pcre
    GIT_REPOSITORY    https://github.com/mar-file-system/sqlite3-pcre.git
    GIT_TAG           master
    GIT_SHALLOW       1
    PREFIX            ${CMAKE_BINARY_DIR}/
    STEP_TARGETS      build
    INSTALL_COMMAND   ""
  )
elseif ("${GIT_DL_METHOD}" STREQUAL "ssh")
  ExternalProject_Add(sqlite3-pcre
    GIT_REPOSITORY    git@github.com:mar-file-system/sqlite3-pcre.git
    GIT_TAG           master
    GIT_SHALLOW       1
    PREFIX            ${CMAKE_BINARY_DIR}/
    STEP_TARGETS      build
    INSTALL_COMMAND   ""
  )
endif()

# everyone includes (-I) these paths, whether or not they need them
set(COMMON_INCLUDES ${CMAKE_SOURCE_DIR} ${XATTR_INCLUDEDIR} ${UUID_INCLUDEDIR} ${SQLITE3_INCLUDEDIR} ${pcre_PREFIX} ${thpool_PREFIX})
include_directories(${COMMON_INCLUDES})

# dfw includes utils.c for some reason, so ${CMAKE_BINARY_DIR} needs to be included
include_directories(${CMAKE_BINARY_DIR})

# utils.c needs an include path change in OpenSUSE
set(XATTR_H "${XATTR_HEADER}")
configure_file(${CMAKE_SOURCE_DIR}/utils.c.in ${CMAKE_BINARY_DIR}/utils.c @ONLY)

# create the GUFI library, which contains all of the common source files
set(GUFI_SOURCES bf.c dbutils.c structq.c)
add_library(GUFI STATIC ${GUFI_SOURCES} ${CMAKE_BINARY_DIR}/utils.c)
add_dependencies(GUFI C-Thread-Pool sqlite3-pcre)
install(TARGETS GUFI DESTINATION lib)

# set of libraries needed by the final binaries
set(COMMON_LIBRARIES GUFI ${pcre_PREFIX}-build/libsqlite3-pcre.a sqlite3 pcre ${thpool_PREFIX}/C-Thread-Pool-build/libthpool.a Threads::Threads m)

# This function builds executables using the
# provided file names (SOURCE is an optional
# argument, so it does not show up in the
# arguments list).
#
# PATH is the installation directory.
#
# There will be duplicate targets if two files
# share the same name.
#
# ex:
#     build_and_install(bin TRUE file1.c file2.cpp)
#        -> compiles file1.c into file1
#        -> compiles file2.cpp into file2
#        -> installs file1 and file2 into bin
#
function(build_and_install PATH LINK_GUFI)
  foreach(SOURCE ${ARGN})
    # get the filename only (no path or extension)
    string(REGEX REPLACE "\\.[^.]*$" "" TARGET ${SOURCE})

    # create the target for building the executable
    add_executable(${TARGET} ${SOURCE})

    # link GUFI libraries if needed
    if (LINK_GUFI)
      target_link_libraries(${TARGET} ${COMMON_LIBRARIES})
    endif()

    # install the executable
    install(TARGETS ${TARGET} DESTINATION ${PATH})
  endforeach()
endfunction()

# build and install the binaries that need to link with GUFI
set(BIN_SOURCES
  bfq.c
  bfti.c
  bfwi.c
  bfwreaddirplus2db.c
  dfw.c
  make_testdirs.c
  querydb.c
  querydbn.c)

# add C++ sources if C++ support is available
if (CMAKE_CXX_COMPILER)
  set(BIN_SOURCES ${BIN_SOURCES} make_testtree.cpp)
endif()

# build the main binaries
build_and_install(bin TRUE ${BIN_SOURCES})

# build binaries that do not need to link with GUFI
set(MISC_SOURCES tsmtime2epoch.c tsmepoch2time.c)
build_and_install(bin FALSE ${MISC_SOURCES})

# search for FUSE on Linux, and OSXFUSE on OSX
if (UNIX AND NOT APPLE AND NOT CYGWIN)
  pkg_search_module(FUSE fuse)
  message(STATUS "FUSE headers located in ${FUSE_INCLUDEDIR}")
  message(STATUS "FUSE libraries located in ${FUSE_LIBDIR}")

  set(MY_FUSE_FOUND YES)
  set(MY_FUSE_CFLAGS ${FUSE_CFLAGS})
  set(MY_FUSE_LDFLAGS ${FUSE_LDFLAGS})
elseif (APPLE)
  pkg_search_module(OSXFUSE osxfuse)

  message(STATUS "OSXFUSE headers located in ${OSXFUSE_INCLUDEDIR}")
  message(STATUS "OSXFUSE libraries located in ${OSXFUSE_LIBDIR}")

  set(MY_FUSE_FOUND YES)
  set(MY_FUSE_CFLAGS ${OSXFUSE_CFLAGS})
  set(MY_FUSE_LDFLAGS ${OSXFUSE_LDFLAGS})
endif()

# only build bffuse and bfresultfuse if FUSE is found
if (MY_FUSE_FOUND)
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

  # install the fuse binaries in ${CMAKE_INSTALL_PREFIX}/bin
  install(TARGETS bffuse bfresultfuse DESTINATION bin)
else()
  message(STATUS "FUSE not found. Not building bffuse and bfresultfuse.")
endif()
set(FUSE_SOURCES bffuse.c bfresultfuse.c)

# only build bfmi if MySQL is found
pkg_search_module(MYSQL mysql)
if (MYSQL_FOUND)
  message(STATUS "MySQL headers located in ${MYSQL_INCLUDEDIR}")
  message(STATUS "MySQL libraries located in ${MYSQL_LIBDIR}")

  string(REPLACE ";" " " MYSQL_CFLAGS "${MYSQL_CFLAGS}")
  string(REPLACE ";" " " MYSQL_LDFLAGS "${MYSQL_LDFLAGS}")

  # create the bfmi executable
  add_executable(bfmi bfmi.c)
  set_target_properties(bfmi PROPERTIES COMPILE_FLAGS "${MYSQL_CFLAGS}")
  target_link_libraries(bfmi ${COMMON_LIBRARIES} ${MYSQL_LDFLAGS})

  # install the MySQL binaries in ${CMAKE_INSTALL_PREFIX}/bin
  install(TARGETS bfmi DESTINATION bin)
else()
  message(STATUS "MySQL not found. Not building bfmi.")
endif()
set(MYSQL_SOURCES bfmi.c)

# search for DB2
pkg_search_module(DB2 db2)
if (DB2_FOUND)
  message(STATUS "DB2 headers located in ${DB2_INCLUDEDIR}")
  message(STATUS "DB2 libraries located in ${DB2_LIBDIR}")

  string(REPLACE ";" " " DB2_CFLAGS "${DB2_CFLAGS}")
  string(REPLACE ";" " " DB2_LDFLAGS "${DB2_LDFLAGS}")

  # create the executable

  # install the DB2 binarys in ${CMAKE_INSTALL_PREFIX}/bin
else()
  message(STATUS "DB2 not found.")
endif()
set(DB2_SOURCES)

# list of scripts that act as tools
set(TOOLS example_run gufi_client gufi_find gufi_stats gufi_ls query_builder.py)

set(PARAMIKO OFF CACHE BOOL "Whether or not to download and install paramiko")
if (PARAMIKO)
  # download paramiko
  set(PARAMIKO_PREFIX ${CMAKE_BINARY_DIR}/src)
  if ("${GIT_DL_METHOD}" STREQUAL "https")
    ExternalProject_Add(paramiko
      GIT_REPOSITORY    https://github.com/paramiko/paramiko.git
      GIT_TAG           cf7d49d66f3b1fbc8b0853518a54050182b3b5eb
      GIT_SHALLOW       1
      PREFIX            ${CMAKE_BINARY_DIR}/
      PATCH_COMMAND     git apply ${CMAKE_SOURCE_DIR}/contrib/paramiko-cf7d49d-hostbased.patch
      CONFIGURE_COMMAND ""
      BUILD_COMMAND     ""
      INSTALL_COMMAND   ${CMAKE_COMMAND} -E copy_directory ${CMAKE_BINARY_DIR}/src/paramiko ${CMAKE_INSTALL_PREFIX}/paramiko
    )
  elseif ("${GIT_DL_METHOD}" STREQUAL "ssh")
    ExternalProject_Add(paramiko
      GIT_REPOSITORY    git@github.com:paramiko/paramiko.git
      GIT_TAG           cf7d49d66f3b1fbc8b0853518a54050182b3b5eb
      GIT_SHALLOW       1
      PREFIX            ${CMAKE_BINARY_DIR}/
      PATCH_COMMAND     git apply ${CMAKE_SOURCE_DIR}/contrib/paramiko-cf7d49d-hostbased.patch
      CONFIGURE_COMMAND ""
      BUILD_COMMAND     ""
      INSTALL_COMMAND   ${CMAKE_COMMAND} -E copy_directory ${CMAKE_BINARY_DIR}/src/paramiko ${CMAKE_INSTALL_PREFIX}/paramiko
    )
  endif()

  # add gufi_client.in to the list of tools
  set(TOOLS ${TOOLS} gufi_client)
endif()

# copy and install scripts that act like binaries
foreach(TOOL ${TOOLS})
  # copy the tool into the build directory for easy access
  configure_file(${CMAKE_SOURCE_DIR}/${TOOL} ${CMAKE_BINARY_DIR}/${TOOL} COPYONLY)

  # install these tools into ${CMAKE_INSTALL_PREFIX}/bin
  install(PROGRAMS ${TOOL} DESTINATION bin)
endforeach()

# copy the scripts directory into ${CMAKE_BINARY_DIR}
file(COPY scripts DESTINATION ${CMAKE_BINARY_DIR})

# enable "make test"
enable_testing()

# recurse down into the test subdirectory
add_subdirectory(test)

# define "make gary"
# build tarball out of ${CMAKE_SOURCE_DIR}

# collect all files into variables
set(SOURCES ${GUFI_SOURCES} utils.c.in ${BIN_SOURCES} ${MISC_SOURCES} ${FUSE_SOURCES} ${MYSQL_SOURCES} ${DB2_SOURCES})
set(HEADERS bf.h dbutils.h mygpfs.h structq.h utils.h)
set(OTHER_FILES
  CMakeLists.txt
  INSTALL
  LICENSE.txt
  Makefile
  NOTES.txt
  README
  README.md
  TBD.txt
  gpfs-scan-listnew-out.c
  gpfs-scan-listnew.c
  modu.c
  mygpfs.h
  runtsm
  triell.c)
set(PATHS
  contrib
  docs
  include
  scripts
  test
  C-Thread-Pool
  sqlite3-pcre
  googletest)

# set the output nemae
set(TARBALL_NAME "${CMAKE_BINARY_DIR}/gufi.tar.gz")

# generate the tarball
add_custom_target(gary COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_BINARY_DIR}/src/C-Thread-Pool ${CMAKE_SOURCE_DIR}/C-Thread-Pool
                       COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_BINARY_DIR}/src/sqlite3-pcre ${CMAKE_SOURCE_DIR}/sqlite3-pcre
                       COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_BINARY_DIR}/googletest-src ${CMAKE_SOURCE_DIR}/googletest
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/C-Thread-Pool/.git
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/sqlite3-pcre/.git
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/googletest/.git
                       COMMAND ${CMAKE_COMMAND} -E tar "czf" ${TARBALL_NAME} ${PATHS} ${SOURCES} ${HEADERS} ${TOOLS} ${OTHER_FILES} WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/C-Thread-Pool
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/sqlite3-pcre
                       COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/googletest
                       DEPENDS C-Thread-Pool sqlite3-pcre test/googletest)
