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



set(SERVER_CLIENT_MAN_PAGES
  gufi_find.1
  gufi_ls.1
  gufi_stat.1
  gufi_stats.1
)

set(SERVER_MAN_PAGES
  bfwreaddirplus2db.1
  gufi_dir2index.1
  gufi_dir2trace.1
  gufi_query.1
  gufi_trace2index.1
  gufi_treesummary.1
  querydbs.1
)

# man pages for executables found in the server/local install
foreach(MAN_PAGE ${SERVER_MAN_PAGES} ${SERVER_CLIENT_MAN_PAGES})
  set(MAN_PAGE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/${MAN_PAGE}.in")
  set(MAN_PAGE_DST "${CMAKE_CURRENT_BINARY_DIR}/${MAN_PAGE}")
  configure_file(${MAN_PAGE_SRC} ${MAN_PAGE_DST} @ONLY)
  string(REGEX MATCH "[^.]+$" SECTION "${MAN_PAGE}")
  install(FILES "${MAN_PAGE_DST}"
    DESTINATION share/man/man${SECTION}
    COMPONENT Server)
endforeach()

# man pages for executables found in the client
if (CLIENT)
  foreach(MAN_PAGE ${SERVER_CLIENT_MAN_PAGES})
    set(MAN_PAGE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/${MAN_PAGE}.in")
    set(MAN_PAGE_DST "${CMAKE_CURRENT_BINARY_DIR}/${MAN_PAGE}")
    configure_file(${MAN_PAGE_SRC} ${MAN_PAGE_DST} @ONLY)
    string(REGEX MATCH "[^.]+$" SECTION "${MAN_PAGE}")
    install(FILES "${MAN_PAGE_DST}"
      DESTINATION share/man/man${SECTION}
      COMPONENT Client)
  endforeach()
endif()

# CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION needs to be sent all the way to the root scope
# in order to not add /usr/share/man and /usr/share/man/man1 into the RPMs
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/usr/share/man")
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/usr/share/man/man1")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION}" PARENT_SCOPE)
