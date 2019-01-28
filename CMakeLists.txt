cmake_minimum_required(VERSION 3.0.0)
project(GUFI LANGUAGES C CXX)

# define the DEBUG macro when building in debug mode
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")

# enable C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# include (-I) the source directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# need the PkgConfig module for pkg_search_module
find_package(PkgConfig REQUIRED)

# need pthread
find_package(Threads REQUIRED)

# load the ExternalProject module
include(ExternalProject)

# download, build, and install C-Thread-Pool
# installation needs to happen here so that the executables
# have a fixed dynamic library location to link to
set(thpool_PREFIX ${CMAKE_INSTALL_PREFIX}/C-Thread-Pool)
ExternalProject_Add(C-Thread-Pool
  GIT_REPOSITORY    https://github.com/calccrypto/C-Thread-Pool.git
  GIT_TAG           cmake
  GIT_SHALLOW       1
  PREFIX            ${CMAKE_BINARY_DIR}/
  CMAKE_ARGS        -DCMAKE_INSTALL_PREFIX=${thpool_PREFIX}
)
include_directories(${thpool_PREFIX}/include)             # -Ipath/to/C-Thread-Pool/include
link_directories(${thpool_PREFIX}/lib)                    # -Lpath/to/C-Thread-Pool/lib

# set the install rpath so the executables that link
# against C-Thread-Pool can find it after being installed
set(CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_RPATH} ${thpool_PREFIX}/lib)

# search for sqlite3 installation
pkg_search_module(SQLITE3 REQUIRED sqlite3)
include_directories(AFTER SYSTEM ${SQLITE3_INCLUDE_DIRS}) # -Ipath/to/sqlite3/include
link_directories(${SQLITE3_LIBRARY_DIRS})                 # -Lpath/to/sqlite3/lib
message(STATUS "SQLITE3 headers located in ${SQLITE3_INCLUDEDIR}")
message(STATUS "SQLITE3 libraries located in ${SQLITE3_LIBDIR}")

# download, build, and install sqlite3-pcre
# installation needs to happen here so that the executables
# have a fixed path to load sqlite3-pcre from
set(pcre_PREFIX     ${CMAKE_INSTALL_PREFIX}/sqlite3-pcre)
ExternalProject_Add(sqlite3-pcre
  GIT_REPOSITORY    https://github.com/mar-file-system/sqlite3-pcre.git
  GIT_TAG           master
  GIT_SHALLOW       1
  PREFIX            ${CMAKE_BINARY_DIR}/
  CMAKE_ARGS        -DCMAKE_INSTALL_PREFIX=${pcre_PREFIX}
)

# create the GUFI library, which contains all of the common source files
set_source_files_properties(dbutils.c PROPERTIES COMPILE_FLAGS "-DPCRE=${pcre_PREFIX}/sqlite3-pcre.so")
add_library(GUFI STATIC bf.c dbutils.c structq.c utils.c)
add_dependencies(GUFI C-Thread-Pool)
install(TARGETS GUFI DESTINATION lib)

# pack common values into variables
set(COMMON_DEPENDENCIES GUFI C-Thread-Pool)
set(COMMON_LIBRARIES GUFI sqlite3 thpool Threads::Threads)

# This function builds the files whose source
# file name matches the pattern NAME.EXT (NAME
# is an optional argument, so it does not show
# up in the arguments list). PATH is the
# installation directory.
#
# ex:
#     build_and_install(bin c file1 file2)
#        -> compiles file1.c into file1
#        -> compiles file2.c into file2
#        -> installs file1 and file2 into bin
#
function(build_and_install PATH EXT)
  foreach(NAME ${ARGN})
    # build the executable
    add_executable(${NAME} ${NAME}.${EXT})
    add_dependencies(${NAME} ${COMMON_DEPENDENCIES})
    target_link_libraries(${NAME} ${COMMON_LIBRARIES})

    # install the executable in ${CMAKE_INSTALL_PREFIX}/bin
    install(TARGETS ${NAME} DESTINATION ${PATH})
  endforeach()
endfunction()

# build and install the binaries that have C sources
build_and_install(bin c
  bfq
  bfti
  bfwi
  bfwreaddirplus2db
  dfw
  make_testdirs
  querydb
  querydbn)

# build and install the binaries that have C++ sources
build_and_install(bin cpp
  make_testtree)

# build the miscellaneous binaries
# this might have to be split into individual
# instructions as the number of miscellaneous
# binaries grows
foreach(NAME tsmtime2epoch tsmepoch2time)
  # build the executable
  add_executable(${NAME} ${NAME}.c)

  # install the executable in ${CMAKE_INSTALL_PREFIX}/bin
  install(TARGETS ${NAME} DESTINATION bin)
endforeach()

# only build bffuse and bfresultfuse if FUSE is found
pkg_search_module(FUSE fuse)
if (FUSE_FOUND)
  message(STATUS "FUSE headers located in ${FUSE_INCLUDEDIR}")
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

# tell CMake to search the contrib directory for FindMYSQL.cmake and mysql-config.cmake
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/contrib)

# only build bfmi if MySQL is found
pkg_search_module(MYSQL mysql)
if (MYSQL_FOUND)
  message(STATUS "MySQL headers located in ${MYSQL_INCLUDEDIR}")
  message(STATUS "MySQL libraries located in ${MYSQL_LIBDIR}")

  # create the bfmi executable
  add_executable(bfmi bfmi.c)
  add_dependencies(bfmi ${COMMON_DEPENDENCIES})
  target_link_libraries(bfmi ${COMMON_LIBRARIES} mysql)

  # install the MySQL binaries in ${CMAKE_INSTALL_PREFIX}/bin
  install(TARGETS bfmi DESTINATION bin)
else()
  message(STATUS "MySQL not found. Not building bfmi.")
endif()

# copy and install scripts that act like binaries
foreach(SCRIPT example_run gufi_find gufi_stats gufi_ls query_builder.py)
  # copy the scropt into the build directory for easy access
  configure_file(${CMAKE_SOURCE_DIR}/${SCRIPT} ${CMAKE_BINARY_DIR}/${SCRIPT} COPYONLY)

  # install these scripts into ${CMAKE_INSTALL_PREFIX}/bin
  install(PROGRAMS ${SCRIPT} DESTINATION bin)
endforeach()

# recurse down into the test subdirectory
add_subdirectory(test)
