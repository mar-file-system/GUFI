cmake_minimum_required(VERSION 3.10.0)

get_filename_component(CMAKE_SOURCE_DIR "${CMAKE_SOURCE_DIR}" REALPATH)
get_filename_component(CMAKE_BINARY_DIR "${CMAKE_BINARY_DIR}" REALPATH)
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds not allowed.")
endif()

project(beegfs-index VERSION 1.0.0 LANGUAGES C)

set(BEEGFS_INDEX_VERSION "" CACHE STRING "Defaults to current git version.")
if(BEEGFS_INDEX_VERSION STREQUAL "")
  execute_process(COMMAND git describe --tags --match "*.*" --abbrev=10
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE BEEGFS_INDEX_VERSION_FROM_GIT
    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  string(REGEX REPLACE "^v" "" BEEGFS_INDEX_VERSION_FROM_GIT "${BEEGFS_INDEX_VERSION_FROM_GIT}")
  if(BEEGFS_INDEX_VERSION_FROM_GIT STREQUAL "")
    message(FATAL_ERROR "Cannot determine BEEGFS_INDEX_VERSION. Specify with `cmake -DBEEGFS_INDEX_VERSION=...`")
  endif()
  set(BEEGFS_INDEX_VERSION ${BEEGFS_INDEX_VERSION_FROM_GIT} CACHE STRING "Defaults to current git version" FORCE)
  set(BEEGFS_INDEX_EPOCH 19)
else()
  set(BEEGFS_INDEX_EPOCH 20)
endif()
string(REPLACE "~" "-" FILE_NAME_VERSION "${BEEGFS_INDEX_VERSION}")
string(REPLACE "-" "~" BEEGFS_INDEX_VERSION "${BEEGFS_INDEX_VERSION}")
set(BEEGFS_INDEX_VERSION ${BEEGFS_INDEX_VERSION} CACHE STRING "" FORCE)
set(BEEGFS_INDEX_EPOCH   ${BEEGFS_INDEX_EPOCH}   CACHE STRING "" FORCE)
message(STATUS "BEEGFS_INDEX_VERSION: ${BEEGFS_INDEX_VERSION}")

if(DEFINED ENV{BEEGFS_DEBUG} AND "$ENV{BEEGFS_DEBUG}" STREQUAL "1")
  set(CMAKE_BUILD_TYPE Debug)
else()
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS         "${CMAKE_C_FLAGS}         -D_XOPEN_SRC")
set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}   -DDEBUG -Wall")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2")

# zig cc (clang) promotes implicit function declarations to errors in C99+ mode.
# Upstream GUFI code relies on gcc's lenient behavior, so downgrade to a warning.
if(CMAKE_C_COMPILER_ID MATCHES "Clang")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-implicit-function-declaration")
endif()

option(QPTPOOL_SWAP     "Enable QueuePerThreadPool swapping"     ON)
# SECURITY (load-bearing): keep this OFF. With it OFF, gufi_query is built
# without the --read-write (-w) flag and opens every index database read-only,
# so no query it runs — including raw SQL any user can pass via `beegfs index
# query` — can modify an on-disk index. This is the primary safeguard for SQL
# that reaches the index; the Go-side query escaping in beegfs-go (sqlQuote /
# the Predicate constructors) is defense-in-depth, not a substitute. Turning
# this ON removes the safeguard for every gufi_query caller. CI asserts it stays
# off (.github/workflows/check.yml passes -DALLOW_DB_WRITES=Off).
option(ALLOW_DB_WRITES  "Allow gufi_query to write to databases" OFF)
option(DEP_USE_JEMALLOC "Link with jemalloc"                     ON)
option(CROSS_COMPILE    "Cross-compiling for a foreign arch"     OFF)

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_PREFIX "/opt/beegfs" CACHE PATH "" FORCE)
set(CPACK_PACKAGING_INSTALL_PREFIX "/opt/beegfs")

get_filename_component(GUFI_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(GUFI_SRC   "${GUFI_ROOT}/src")
set(BEEGFS_SRC "${GUFI_ROOT}/contrib/plugins/beegfs")
set(CMAKE_MODULE_PATH "${GUFI_ROOT}/contrib/cmake" ${CMAKE_MODULE_PATH})

find_package(PkgConfig REQUIRED)
find_package(Threads   REQUIRED)
find_package(XATTR     REQUIRED)

if(XATTR_ATTR_XATTR_HEADER)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_ATTR_XATTR_H")
endif()
if(XATTR_SYS_XATTR_HEADER)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_SYS_XATTR_H")
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DCONFIG_GNU_XATTRS")

include(CheckSymbolExists)
set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
check_symbol_exists(statx      "sys/stat.h" HAVE_STATX)
check_symbol_exists(localtime_r "time.h"    LOCALTIME_R)
if(HAVE_STATX)
  add_definitions(-D_GNU_SOURCE -DHAVE_STATX=1)
endif()

pkg_search_module(PCRE2 REQUIRED libpcre2-8)
add_definitions(-DPCRE2_CODE_UNIT_WIDTH=8) # needed by external_copy.h
pkg_search_module(ZLIB  zlib)
if(ZLIB_FOUND)
  add_definitions(-DHAVE_ZLIB=1)
endif()

set(DEP_DOWNLOAD_PREFIX "${GUFI_ROOT}/contrib/deps" CACHE PATH   "")
set(DEP_BUILD_PREFIX    "${CMAKE_BINARY_DIR}/builds" CACHE PATH   "")
set(DEP_INSTALL_PREFIX  "${CMAKE_BINARY_DIR}/deps"   CACHE PATH   "")
set(DEP_BUILD_THREADS   "1"                          CACHE STRING "")
set(_dep_opts --threads "${DEP_BUILD_THREADS}")

set(JEMALLOC "")
if(DEP_USE_JEMALLOC)
  pkg_search_module(JEMALLOC_PKG jemalloc)
  if(JEMALLOC_PKG_FOUND)
    if(EXISTS "${JEMALLOC_PKG_STATIC_LINK_LIBRARIES}")
      set(JEMALLOC "${JEMALLOC_PKG_STATIC_LINK_LIBRARIES}")
    else()
      get_filename_component(_jext "${JEMALLOC_PKG_LINK_LIBRARIES}" EXT)
      string(REPLACE "${_jext}" ".a" JEMALLOC "${JEMALLOC_PKG_LINK_LIBRARIES}")
    endif()
  endif()
  if(NOT EXISTS "${JEMALLOC}")
    set(_dep_opts ${_dep_opts} --jemalloc)
    set(JEMALLOC "${DEP_INSTALL_PREFIX}/jemalloc/lib/libjemalloc.a")
  endif()
endif()

add_custom_target(install_dependencies ALL
  COMMAND "${GUFI_ROOT}/contrib/deps/install_deps.sh" ${_dep_opts}
          "${DEP_DOWNLOAD_PREFIX}" "${DEP_BUILD_PREFIX}" "${DEP_INSTALL_PREFIX}"
          "${CMAKE_VERSION}" "${CMAKE_SYSTEM_NAME}"
)

set(EXT "so")

include_directories(
  "${GUFI_ROOT}/include"
  "${DEP_INSTALL_PREFIX}/sqlite3/include"
  "${DEP_INSTALL_PREFIX}/sqlite3-pcre"
)
# Use SYSTEM so these become -isystem rather than -I, ensuring any user-specified
# -I paths (e.g. CFLAGS wrapper headers) take precedence in the search order.
include_directories(SYSTEM
  "${XATTR_INCLUDEDIR}"
)

set(COMMON_LIBRARIES
  GUFI
  "${DEP_INSTALL_PREFIX}/sqlite3-pcre/libsqlite3-pcre.a"
  "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.a"
  ${PCRE2_LDFLAGS}
)
if(ZLIB_FOUND)
  list(APPEND COMMON_LIBRARIES z)
endif()
if(DEP_USE_JEMALLOC)
  list(APPEND COMMON_LIBRARIES "${JEMALLOC}")
endif()
list(APPEND COMMON_LIBRARIES Threads::Threads m dl)

set(GUFI_SOURCES
  "${GUFI_SRC}/BottomUp.c"        "${GUFI_SRC}/OutputBuffers.c"
  "${GUFI_SRC}/QueuePerThreadPool.c" "${GUFI_SRC}/SinglyLinkedList.c"
  "${GUFI_SRC}/addqueryfuncs.c"   "${GUFI_SRC}/bf.c"
  "${GUFI_SRC}/compress.c"        "${GUFI_SRC}/dbutils.c"
  "${GUFI_SRC}/debug.c"           "${GUFI_SRC}/descend.c"
  "${GUFI_SRC}/external_attach.c" "${GUFI_SRC}/external_copy.c"
  "${GUFI_SRC}/gufi_vt.c"
  "${GUFI_SRC}/histogram.c"       "${GUFI_SRC}/index.c"
  "${GUFI_SRC}/outfiles.c"
  "${GUFI_SRC}/path_list.c"       "${GUFI_SRC}/plugin.c"
  "${GUFI_SRC}/popen_argv.c"      "${GUFI_SRC}/print.c"
  "${GUFI_SRC}/rollup.c"
  "${GUFI_SRC}/run_vt.c"          "${GUFI_SRC}/str.c"
  "${GUFI_SRC}/template_db.c"     "${GUFI_SRC}/trace.c"
  "${GUFI_SRC}/trie.c"            "${GUFI_SRC}/utils.c"
  "${GUFI_SRC}/xattrs.c"
)
if(QPTPOOL_SWAP)
  list(APPEND GUFI_SOURCES "${GUFI_SRC}/swap.c")
endif()

add_library(GUFI STATIC ${GUFI_SOURCES})
add_dependencies(GUFI install_dependencies)
target_compile_definitions(GUFI PUBLIC SQLITE_CORE)
if(QPTPOOL_SWAP)
  target_compile_definitions(GUFI PUBLIC QPTPOOL_SWAP)
endif()
if(LOCALTIME_R)
  set_source_files_properties("${GUFI_SRC}/addqueryfuncs.c" PROPERTIES COMPILE_FLAGS -DLOCALTIME_R)
endif()
set_source_files_properties("${GUFI_SRC}/bf.c" PROPERTIES COMPILE_FLAGS "-DGUFI_VERSION=\"${BEEGFS_INDEX_VERSION}\"")

foreach(_exe gufi_dir2index gufi_sqlite3 gufi_treesummary)
  add_executable(${_exe} "${GUFI_SRC}/${_exe}.c")
  target_link_libraries(${_exe} ${COMMON_LIBRARIES})
  add_dependencies(${_exe} GUFI)
  install(TARGETS ${_exe} DESTINATION bin/index)
endforeach()
set_target_properties(gufi_dir2index PROPERTIES ENABLE_EXPORTS TRUE)

add_library(gufi_query_lib OBJECT
  "${GUFI_SRC}/gufi_query/PoolArgs.c"       "${GUFI_SRC}/gufi_query/aggregate.c"
  "${GUFI_SRC}/gufi_query/external_attach.c" "${GUFI_SRC}/gufi_query/gqw.c"
  "${GUFI_SRC}/gufi_query/handle_sql.c"     "${GUFI_SRC}/gufi_query/process_queries.c"
  "${GUFI_SRC}/gufi_query/processdir.c"     "${GUFI_SRC}/gufi_query/query.c"
  "${GUFI_SRC}/gufi_query/query_replacement.c" "${GUFI_SRC}/gufi_query/query_user_strs.c"
)
add_dependencies(gufi_query_lib GUFI)
target_compile_definitions(gufi_query_lib PUBLIC SQLITE_CORE)

add_executable(gufi_query "${GUFI_SRC}/gufi_query/main.c" $<TARGET_OBJECTS:gufi_query_lib>)
target_link_libraries(gufi_query ${COMMON_LIBRARIES})
add_dependencies(gufi_query GUFI)
# SECURITY: see the ALLOW_DB_WRITES option above — defining this compiles the
# --read-write flag into gufi_query and lets it open indexes for writing.
if(ALLOW_DB_WRITES)
  target_compile_definitions(gufi_query PUBLIC ALLOW_DB_WRITES=1)
endif()
set_target_properties(gufi_query PROPERTIES ENABLE_EXPORTS TRUE)
install(TARGETS gufi_query DESTINATION bin/index)

foreach(_plugin beegfs_indexing beegfs_querying)
  add_library(${_plugin} SHARED "${BEEGFS_SRC}/${_plugin}.c" "${BEEGFS_SRC}/beegfs_shared.c")
  add_dependencies(${_plugin} install_dependencies)
  target_link_libraries(${_plugin} "${DEP_INSTALL_PREFIX}/sqlite3/lib/libsqlite3.${EXT}")
  set_target_properties(${_plugin} PROPERTIES
    BUILD_WITH_INSTALL_RPATH FALSE
    INSTALL_RPATH "$ORIGIN"
    BUILD_RPATH "${DEP_INSTALL_PREFIX}/sqlite3/lib")
  install(TARGETS ${_plugin} DESTINATION lib)
endforeach()

install(DIRECTORY "${DEP_INSTALL_PREFIX}/sqlite3/lib/"
  DESTINATION lib
  FILES_MATCHING PATTERN "libsqlite3.so.*"
  PATTERN "pkgconfig" EXCLUDE
)

install(FILES "${GUFI_ROOT}/beegfs_deploy/LICENSE.md"
  DESTINATION /usr/share/doc/beegfs-index
)

install(FILES "${GUFI_ROOT}/NOTICE.md"
  DESTINATION /usr/share/doc/beegfs-index
)

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/config/beegfs.index.example"
  DESTINATION /etc/beegfs/index)

if("${PACKAGE_TYPE}" STREQUAL "deb")
  find_program(DPKG dpkg REQUIRED)
  if(DEFINED DEB_ARCH AND NOT "${DEB_ARCH}" STREQUAL "")
    set(ARCH "${DEB_ARCH}")
  else()
    execute_process(COMMAND ${DPKG} --print-architecture OUTPUT_VARIABLE ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
  set(CPACK_OUTPUT_FILE_PREFIX           "${CMAKE_SOURCE_DIR}/../packages")
  set(CPACK_GENERATOR                   "DEB")
  set(CPACK_DEBIAN_PACKAGE_NAME         "beegfs-index")
  set(CPACK_DEBIAN_PACKAGE_VERSION      "${BEEGFS_INDEX_VERSION}")
  set(CPACK_DEBIAN_PACKAGE_EPOCH        "${BEEGFS_INDEX_EPOCH}")
  set(CPACK_DEBIAN_PACKAGE_MAINTAINER   "ThinkParQ")
  set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "${ARCH}")
  set(CPACK_DEBIAN_PACKAGE_DEPENDS      "libpcre2-8-0, libattr1, zlib1g")
  set(CPACK_PACKAGE_FILE_NAME           "beegfs-index_${FILE_NAME_VERSION}_${ARCH}")
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")
    set(CPACK_DEB_COMPONENT_INSTALL ON)
    set(CPACK_DEBIAN_DEBUGINFO_PACKAGE ON)
    set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_NAME "beegfs-index-dbg")
  endif()
  include(CPack)

elseif("${PACKAGE_TYPE}" STREQUAL "rpm")
  find_program(UNAME uname REQUIRED)
  if(DEFINED RPM_ARCH AND NOT "${RPM_ARCH}" STREQUAL "")
    set(ARCH "${RPM_ARCH}")
  else()
    execute_process(COMMAND ${UNAME} -m OUTPUT_VARIABLE ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
  set(CPACK_OUTPUT_FILE_PREFIX        "${CMAKE_SOURCE_DIR}/../packages")
  set(CPACK_GENERATOR                "RPM")
  set(CPACK_PACKAGE_NAME             "beegfs-index")
  set(CPACK_PACKAGE_VERSION          "${BEEGFS_INDEX_VERSION}")
  set(CPACK_RPM_PACKAGE_EPOCH        "${BEEGFS_INDEX_EPOCH}")
  set(CPACK_RPM_PACKAGE_ARCHITECTURE "${ARCH}")
  set(CPACK_RPM_PACKAGE_LICENSE      "BSD")
  set(CPACK_RPM_PACKAGE_VENDOR       "ThinkParQ")
  set(CPACK_RPM_PACKAGE_REQUIRES     "pcre2, libattr, zlib")
  set(CPACK_RPM_PACKAGE_AUTOREQ      OFF)
  set(CPACK_PACKAGE_RELOCATABLE      OFF)
  set(CPACK_RPM_FILE_NAME            "beegfs-index-${FILE_NAME_VERSION}.${ARCH}.rpm")
  set(CPACK_RPM_USER_FILELIST
    "%license /usr/share/doc/beegfs-index/LICENSE.md"
    "%license /usr/share/doc/beegfs-index/NOTICE.md")
  if(CROSS_COMPILE)
    # When cross-compiling, the host strip binary can't parse the target arch's ELF files.
    # Suppress rpmbuild's brp-strip post-install script to avoid a fatal packaging error.
    set(CPACK_RPM_SPEC_MORE_DEFINE "%define __strip /bin/true")
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")
    set(CPACK_RPM_DEBUGINFO_PACKAGE ON)
    set(CPACK_RPM_DEBUGINFO_FILE_NAME RPM-DEFAULT)
    set(CPACK_RPM_BUILD_SOURCE_DIRS_PREFIX "/usr/src/debug")
  endif()
  include(CPack)
endif()
