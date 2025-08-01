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



include(ProcessorCount)
ProcessorCount(CORES)

set(CONFIG_DEFAULT "/etc/GUFI/config")

function(config_file var_name type)
  set("${var_name}" "${CONFIG_DEFAULT}" CACHE FILEPATH "Where high level GUFI tools will look for the ${type} configuration")
  if (NOT IS_ABSOLUTE "${${var_name}}")
    message(FATAL_ERROR "${var_name} must be set to absolute path")
  endif()
  if (EXISTS "${${var_name}}")
    if (APPLE OR DARWIN)
      execute_process(COMMAND stat -L -f "%HT" "${${var_name}}"
        OUTPUT_VARIABLE CONFIG_FILE_TYPE
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )
      if (NOT CONFIG_FILE_TYPE STREQUAL "Regular File")
        message(FATAL_ERROR "${var_name} must point to a file. Got ${CONFIG_FILE_TYPE}")
      endif()
    else()
      execute_process(COMMAND stat --dereference --format "%F" "${${var_name}}"
        OUTPUT_VARIABLE CONFIG_FILE_TYPE
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )
      if (NOT CONFIG_FILE_TYPE STREQUAL "regular file")
        message(FATAL_ERROR "${var_name} must point to a file. Got ${CONFIG_FILE_TYPE}")
      endif()
    endif()
  endif()
  if (NOT "${${var_name}}" STREQUAL "${CONFIG_DEFAULT}")
    message(WARNING "${var_name} ${${var_name}} may conflict with filesystem package when installing GUFI RPMs")
  endif()
endfunction()

config_file(CONFIG_SERVER "server")
if (CLIENT)
  config_file(CONFIG_CLIENT "client")
endif()

if (CLIENT)
  get_filename_component(CONFIG_CLIENT_PARENT "${CONFIG_CLIENT}" DIRECTORY)

  # client config
  # copy the hard coded client example file
  configure_file(client.example.in "${CMAKE_BINARY_DIR}/client.example" @ONLY)

  # make install and rpm
  install(FILES "${CMAKE_BINARY_DIR}/client.example"
          DESTINATION "${CONFIG_CLIENT_PARENT}"
          PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ GROUP_WRITE WORLD_READ # 664
          COMPONENT Client)
endif()

get_filename_component(CONFIG_SERVER_PARENT "${CONFIG_SERVER}" DIRECTORY)

# server config
# copy the hard coded server example file
configure_file(server.example.in "${CMAKE_BINARY_DIR}/server.example" @ONLY)

# make install and rpm
install(FILES "${CMAKE_BINARY_DIR}/server.example"
        DESTINATION "${CONFIG_SERVER_PARENT}"
        PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ GROUP_WRITE WORLD_READ # 664
        COMPONENT Server)
