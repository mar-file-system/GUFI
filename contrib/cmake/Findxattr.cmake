# Copied from https://gitlab.kitware.com/cmake/community/wikis/doc/tutorials/How-To-Find-Libraries
# and modified to find xattr.h

# - Try to find XAttr
#  XATTR_FOUND - System has xattr
#  XATTR_INCLUDEDIR - The xattr include directories
#  XATTR_LIBRARY - The libraries needed to use xattr
#  XATTR_HEADER - The header to use to include xattr

find_package(PkgConfig)

# normal location
find_path(ATTR_XATTR
  NAMES attr/xattr.h
  PATHS ${CMAKE_INCLUDE_PATH})

# sometime xattr.h shows up in sys
find_path(SYS_XATTR
  NAMES sys/xattr.h
  PATHS ${CMAKE_INCLUDE_PATH})

# attr takes precedence over sys
if (NOT ATTR_XATTR STREQUAL "ATTR_XATTR-NOTFOUND")
  set(XATTR_HEADER "attr")
  set(XATTR_INCLUDE_DIR ${ATTR_XATTR})
elseif(NOT SYS_XATTR STREQUAL "SYS_XATTR-NOTFOUND")
  SET(XATTR_HEADER "sys")
  set(XATTR_INCLUDE_DIR ${SYS_XATTR})
endif()
file(TO_NATIVE_PATH "${XATTR_HEADER}/xattr.h" XATTR_HEADER)

# XATTR_LIB_DIR doesn't seem to matter
find_path(XATTR_LIB_DIR
  NAMES libattr.so
  PATHS /lib /lib64 /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64 "${CMAKE_LIBRARY_PATH}")

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set XATTR_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(XATTR DEFAULT_MSG
                                  XATTR_HEADER XATTR_INCLUDE_DIR)

mark_as_advanced(XATTR_INCLUDE_DIR)

set(XATTR_LIBDIR "${XATTR_LIB_DIR}")
set(XATTR_INCLUDEDIR "${XATTR_INCLUDE_DIR}")

unset(XATTR_LIB_DIR)
unset(XATTR_INCLUDE_DIR)
