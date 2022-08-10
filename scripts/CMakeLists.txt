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

# list this first so that make install will overwrite these files
if (CLIENT)
  # add the paramiko source for installing
  # keep the trailing / to rename the directory
  install(DIRECTORY ${DEP_BUILD_PREFIX}/paramiko-master/ DESTINATION lib/GUFI/paramiko COMPONENT Client)

  # copy the configuration handling script into both the Server and Client RPMs
  configure_file(gufi_config.py gufi_config.py COPYONLY)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/gufi_config.py DESTINATION bin COMPONENT Client)

  # gufi_common.py is also needed by the client
  configure_file(gufi_common.py gufi_common.py COPYONLY)
  install(FILES ${CMAKE_CURRENT_BINARY_DIR}/gufi_common.py DESTINATION bin COMPONENT Client)

  # create individual client scripts
  set(TOOL find)
  configure_file(gufi_client gufi_client_find @ONLY)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/gufi_client_find  RENAME gufi_find  DESTINATION bin COMPONENT Client)

  set(TOOL ls)
  configure_file(gufi_client gufi_client_ls @ONLY)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/gufi_client_ls    RENAME gufi_ls    DESTINATION bin COMPONENT Client)

  set(TOOL stats)
  configure_file(gufi_client gufi_client_stats @ONLY)
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/gufi_client_stats RENAME gufi_stats DESTINATION bin COMPONENT Client)
endif()

# scripts that act as executables on the server
set(TOOLS
  gufi_find
  gufi_getfattr
  gufi_ls
  gufi_stats)

# python libraries installed into bin for convenience
set(LIBRARIES
  gufi_config.py
  gufi_common.py)

# copy and install scripts that act like binaries
foreach(SCRIPT ${TOOLS} ${LIBRARIES})
  # copy the tools into the build directory for easy access
  configure_file(${SCRIPT} ${SCRIPT} @ONLY)

  # install these scripts into ${CMAKE_INSTALL_PREFIX}/bin
  install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${SCRIPT} DESTINATION bin COMPONENT Server)
endforeach()

# gufi_common.py is not an executable, but is installed in bin for convenience
configure_file(gufi_common.py gufi_common.py @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/gufi_common.py DESTINATION bin COMPONENT Server)

# gufi_jail is used by sshd_config to prevent commands other than gufi_* from running
configure_file(gufi_jail gufi_jail COPYONLY)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/gufi_jail DESTINATION bin COMPONENT Server)

# bash completions for the tools so paths can be tab completed
configure_file(completions completions COPYONLY)
if (EXISTS /etc/bash_completion.d)
  EXECUTE_PROCESS(COMMAND test -w /etc/bash_completion.d RESULT_VARIABLE WRITABLE)
  if (WRITABLE EQUAL 0)
    install(FILES completions DESTINATION /etc/bash_completion.d COMPONENT Server RENAME gufi)
  endif()
endif()
