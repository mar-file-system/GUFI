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



# gufi_* scripts
set(TOOLS
  find
  getfattr
  ls
  stat
  stats
)

foreach(TOOL ${TOOLS})
  set(USER_TOOL "gufi_${TOOL}")
  configure_file("${USER_TOOL}" "${USER_TOOL}" @ONLY)

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${USER_TOOL}" DESTINATION "${BIN}" COMPONENT Server)

  if (CLIENT)
    # client files WILL NOT overwrite server files in build directory
    set(CLIENT_TOOL "${CMAKE_CURRENT_BINARY_DIR}/gufi_client_${TOOL}")
    configure_file("gufi_client" "${CLIENT_TOOL}" @ONLY)

    # client files WILL NOT overwrite server files when installing
    install(PROGRAMS "${CLIENT_TOOL}"
      RENAME "${USER_TOOL}"
      DESTINATION "${BIN}"
      COMPONENT Client) # this is always needed for make package
  endif()
endforeach()

# allow for clients to call gufi_query via a script
if (CLIENT)
  set(TOOL "query.py")
  set(USER_TOOL "gufi_${TOOL}")
  configure_file("${USER_TOOL}" "${USER_TOOL}" @ONLY)
  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${USER_TOOL}" DESTINATION "${BIN}" COMPONENT Server)

  set(CLIENT_TOOL "${CMAKE_CURRENT_BINARY_DIR}/gufi_client_query")
  configure_file("gufi_client" "${CLIENT_TOOL}" @ONLY)
  install(PROGRAMS "${CLIENT_TOOL}"
    RENAME "gufi_query"
    DESTINATION "${BIN}"
    COMPONENT Client) # this is always needed for make package
endif()

# gufi_jail is used by sshd_config to prevent commands other than gufi_* from running
configure_file(gufi_jail gufi_jail @ONLY)
install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/gufi_jail" DESTINATION "${BIN}" COMPONENT Server)

# not prefixed with gufi_ and no client tool
configure_file(querydbs querydbs @ONLY)
install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/querydbs" DESTINATION "${BIN}" COMPONENT Server)

# python files for doing distributed operations (server only)
set(DISTRIBUTED
  gufi_dir2index_distributed    # executable only
  gufi_dir2trace_distributed    # executable only
  gufi_distributed.py           # also executable
  gufi_query_distributed        # executable only
)

foreach(DIST ${DISTRIBUTED})
  configure_file("${DIST}" "${DIST}" @ONLY)

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${DIST}" DESTINATION "${BIN}" COMPONENT Server)
endforeach()

# python libraries installed into bin for convenience (server and client)
set(LIBRARIES
  gufi_config.py                # also executable
  gufi_common.py                # library only
)

foreach(LIBRARY ${LIBRARIES})
  configure_file("${LIBRARY}" "${LIBRARY}" @ONLY)

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${LIBRARY}" DESTINATION "${LIB}" COMPONENT Server)

  # when running make install, client files will not overwrite server files
  if (CLIENT)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${LIBRARY}" DESTINATION "${LIB}" COMPONENT Client)
  endif()
endforeach()

# bash completions for the tools so paths can be tab completed
if (COMPOPT EQUAL 0)
  configure_file(bash_completion bash_completion @ONLY)
  option(BASH_COMPLETION "Whether or not to install bash completion script" On)
  if (BASH_COMPLETION)
    install(FILES bash_completion DESTINATION "/etc/bash_completion.d" COMPONENT Server RENAME gufi)
  endif()
endif()

# CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION needs to be sent all the way to the root scope
# in order to not add /etc/bash_completion.d into the RPMs
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/etc/bash_completion.d")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION}" PARENT_SCOPE)
