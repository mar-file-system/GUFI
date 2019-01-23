cmake_minimum_required(VERSION 3.0.0)
project(GUFI LANGUAGES C CXX)

# enable C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# include the source directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# download and build C-Thread-Pool
set(thpool_PREFIX ${CMAKE_BINARY_DIR}/C-Thread-Pool)

include(ExternalProject)
ExternalProject_Add(C-Thread-Pool
  GIT_REPOSITORY    https://github.com/calccrypto/C-Thread-Pool.git
  GIT_TAG           cmake
  GIT_SHALLOW       1
  PREFIX            ${CMAKE_CURRENT_BINARY_DIR}/
  CMAKE_ARGS        -DCMAKE_INSTALL_PREFIX=${thpool_PREFIX}
)
include_directories(${thpool_PREFIX}/include)             # -Ipath/to/C-Thread-Pool/include
link_directories(${thpool_PREFIX}/lib)                    # -Lpath/to/C-Thread-Pool/lib

# need the PkgConfig module for pkg_search_module
find_package(PkgConfig REQUIRED)

# search for pre-install sqlite3
pkg_search_module(SQLITE3 REQUIRED sqlite3)
include_directories(AFTER SYSTEM ${SQLITE3_INCLUDE_DIRS}) # -Ipath/to/sqlite3/include
link_directories(${SQLITE3_LIBRARY_DIRS})                 # -Lpath/to/sqlite3/lib

# download and build sqlite3-pcre
set(pcre_PREFIX     ${CMAKE_BINARY_DIR}/sqlite3-pcre)

ExternalProject_Add(sqlite3-pcre
  GIT_REPOSITORY    https://github.com/calccrypto/sqlite3-pcre.git
  GIT_TAG           cmake
  GIT_SHALLOW       1
  PREFIX            ${CMAKE_CURRENT_BINARY_DIR}/
  CMAKE_ARGS        -DCMAKE_INSTALL_PREFIX=${pcre_PREFIX}
)
include_directories(${pcre_PREFIX}/include)               # -Ipath/to/sqlite3-pcre/include
link_directories(${pcre_PREFIX}/lib)                      # -Lpath/to/sqlite3-pcre/lib

# put common files into this variable
set(COMMON_SOURCES bf.c dbutils.c structq.c utils.c)

# create the GUFI library, which contains all of the common functions
add_library(GUFI STATIC ${COMMON_SOURCES})
add_dependencies(GUFI C-Thread-Pool)
install(TARGETS GUFI DESTINATION lib)

# put common dependencies into this variable
set(COMMON_DEPENDENCIES GUFI C-Thread-Pool)
set(COMMON_LIBRARIES GUFI sqlite3 thpool pthread)

# create the bfq executable
add_executable(bfq bfq.c)
add_dependencies(bfq ${COMMON_DEPENDENCIES})
target_link_libraries(bfq ${COMMON_LIBRARIES})

# create the bfti executable
add_executable(bfti bfti.c)
add_dependencies(bfti ${COMMON_DEPENDENCIES})
target_link_libraries(bfti ${COMMON_LIBRARIES})

# create the bfwi executable
add_executable(bfwi bfwi.c)
add_dependencies(bfwi ${COMMON_DEPENDENCIES})
target_link_libraries(bfwi ${COMMON_LIBRARIES})

# create the bfreaddirplus2b executable
add_executable(bfwreaddirplus2db bfwreaddirplus2db.c)
add_dependencies(bfwreaddirplus2db ${COMMON_DEPENDENCIES})
target_link_libraries(bfwreaddirplus2db ${COMMON_LIBRARIES})

# create the dfw executable
add_executable(dfw dfw.c)
add_dependencies(dfw ${COMMON_DEPENDENCIES})
target_link_libraries(dfw ${COMMON_LIBRARIES})

# create the make_testdirs executable
add_executable(make_testdirs make_testdirs.c)
add_dependencies(make_testdirs ${COMMON_DEPENDENCIES})
target_link_libraries(make_testdirs ${COMMON_LIBRARIES})

# create the make_testtree executable
add_executable(make_testtree make_testtree.cpp)
add_dependencies(make_testtree ${COMMON_DEPENDENCIES})
target_link_libraries(make_testtree ${COMMON_LIBRARIES})

# create the querydb executable
add_executable(querydb querydb.c)
add_dependencies(querydb ${COMMON_DEPENDENCIES})
target_link_libraries(querydb ${COMMON_LIBRARIES})

# create the querydbn executable
add_executable(querydbn querydbn.c)
add_dependencies(querydbn ${COMMON_DEPENDENCIES})
target_link_libraries(querydbn ${COMMON_LIBRARIES})

# install the binaries in ${CMAKE_INSTALL_PREFIX}/bin
install(TARGETS
  bfq
  bfti
  bfwi
  bfwreaddirplus2db
  dfw
  make_testdirs
  make_testtree
  querydb
  querydbn
  DESTINATION bin)

# only build bffuse and bfresultfuse if FUSE is found
pkg_search_module(FUSE fuse)
if (FUSE_FOUND)
  message(STATUS "FUSE headers located in   ${FUSE_INCLUDEDIR}")
  message(STATUS "FUSE libraries located in ${FUSE_LIBDIR}")

  # create the bffuse executable
  add_executable(bffuse bffuse.c)
  add_dependencies(bffuse ${COMMON_DEPENDENCIES})
  set_target_properties(bffuse PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)
  target_link_libraries(bffuse ${COMMON_LIBRARIES} fuse)

  # create the bfresultfuse executable
  add_executable(bfresultfuse bfresultfuse.c)
  add_dependencies(bfresultfuse ${COMMON_DEPENDENCIES})
  set_target_properties(bfresultfuse PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)
  target_link_libraries(bfresultfuse ${COMMON_LIBRARIES} fuse)

  # install the fuse binaries in ${CMAKE_INSTALL_PREFIX}/bin
  install(TARGETS bffuse bfresultfuse DESTINATION bin)
else()
  message(STATUS "FUSE not found. Not building bffuse and bfresultfuse.")
endif()

# add the test subdirectory
add_subdirectory(test)
