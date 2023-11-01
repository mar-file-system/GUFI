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



cmake_minimum_required(VERSION 3.1.0)

# gufi_* scripts
set(TOOLS
  find
  getfattr
  ls
  stat
  stats
)

# python libraries installed into bin for convenience
set(LIBRARIES
  gufi_config.py # also executable
  gufi_common.py # library only
)

if (CLIENT)
  # add the paramiko source for installing
  # keep the trailing / to rename the directory
  install(DIRECTORY "${DEP_BUILD_PREFIX}/paramiko-master/" DESTINATION lib/GUFI/paramiko COMPONENT Client)
endif()

foreach(TOOL ${TOOLS})
  configure_file("gufi_${TOOL}" "gufi_${TOOL}" @ONLY)

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/gufi_${TOOL}" DESTINATION bin COMPONENT Server)

  # when building, client files WILL NOT overwrite server files
  # when installing, client files WILL overwrite server files
  if (CLIENT)
    configure_file("gufi_client" "gufi_client_${TOOL}" @ONLY)
    install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/gufi_client_${TOOL}"
      RENAME "gufi_${TOOL}"
      DESTINATION bin
      COMPONENT Client)
  endif()
endforeach()

# gufi_jail is used by sshd_config to prevent commands other than gufi_* from running
configure_file(gufi_jail gufi_jail @ONLY)
install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/gufi_jail" DESTINATION bin COMPONENT Server)

foreach(LIB ${LIBRARIES})
  configure_file("${LIB}" "${LIB}" @ONLY)

  install(PROGRAMS "${CMAKE_CURRENT_BINARY_DIR}/${LIB}" DESTINATION bin COMPONENT Server)

  # when running make install, client files will not overwrite server files
  if (CLIENT)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${LIB}" DESTINATION bin COMPONENT Client)
  endif()
endforeach()

# bash completions for the tools so paths can be tab completed
configure_file(bash_completion bash_completion @ONLY)
install(FILES bash_completion DESTINATION "/etc/bash_completion.d" COMPONENT Server RENAME gufi)

# CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION needs to be sent all the way to the root scope
# in order to not add /etc/bash_completion.d into the RPMs
list(APPEND CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/etc/bash_completion.d")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "${CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION}" PARENT_SCOPE)
