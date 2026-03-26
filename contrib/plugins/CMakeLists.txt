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



# See if lustre-devel is installed by checking if the lustreapi.h header is in a standard include path.
find_path(LUSTRE_PLUGIN "lustre/lustreapi.h")
# If so, build the lustre example plugin:
if (LUSTRE_PLUGIN)
  add_library(lustre_plugin SHARED lustre_plugin.c)
  add_dependencies(lustre_plugin install_dependencies)
  target_link_libraries(lustre_plugin lustreapi)
endif()

# common DSI code
add_library(dsi OBJECT
  dsi.c
  "${CMAKE_SOURCE_DIR}/src/SinglyLinkedList.c"
  "${CMAKE_SOURCE_DIR}/src/plugin.c"
  "${CMAKE_SOURCE_DIR}/src/utils.c"
)
set_property(TARGET dsi PROPERTY POSITION_INDEPENDENT_CODE ON)
add_dependencies(dsi install_dependencies)

# DSI indexing plugin
add_library(dsi_indexing SHARED
  dsi_indexing.c
  $<TARGET_OBJECTS:dsi>
)
add_dependencies(dsi_indexing install_dependencies)
install(TARGETS dsi_indexing DESTINATION "${SERVER_LIB}" COMPONENT Server)

# DSI querying plugin
add_library(dsi_querying SHARED
  dsi_querying.c
  $<TARGET_OBJECTS:dsi>
)
target_link_libraries(dsi_querying
  "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}"
)
add_dependencies(dsi_querying install_dependencies)
install(TARGETS dsi_querying DESTINATION "${SERVER_LIB}" COMPONENT Server)
install(TARGETS dsi_querying DESTINATION "${CLIENT_LIB}" COMPONENT Client EXCLUDE_FROM_ALL)

# marfs plugin
set(MARFS_PREFIX "" CACHE PATH "MarFS install prefix")
set(MARFS_SRC    "" CACHE PATH "MarFS source")
if (MARFS_PREFIX AND MARFS_SRC)
  find_package(LibXml2 REQUIRED)
  message(STATUS "MARFS_PREFIX: ${MARFS_PREFIX}")
  message(STATUS "MARFS_SRC: ${MARFS_SRC}")
  message(STATUS "Building marfs plugin.")
  add_library(marfs_plugin SHARED
    marfs_plugin.c
    "${CMAKE_SOURCE_DIR}/src/SinglyLinkedList.c"
    "${CMAKE_SOURCE_DIR}/src/bf.c"
    "${CMAKE_SOURCE_DIR}/src/plugin.c"
    "${CMAKE_SOURCE_DIR}/src/str.c"
    "${CMAKE_SOURCE_DIR}/src/trie.c"
    "${CMAKE_SOURCE_DIR}/src/utils"
    "${CMAKE_SOURCE_DIR}/src/xattrs.c"
  )
  target_include_directories(marfs_plugin SYSTEM PUBLIC "${MARFS_PREFIX}" "${MARFS_SRC}/src" "${LIBXML2_INCLUDE_DIRS}")
  target_link_libraries(marfs_plugin PUBLIC marfs)
  target_link_directories(marfs_plugin PUBLIC "${MARFS_PREFIX}/lib")
  add_dependencies(marfs_plugin install_dependencies)
else()
  message(STATUS "MARFS_PREFIX and MARFS_SRC not set. Not building marfs plugin.")
endif()
