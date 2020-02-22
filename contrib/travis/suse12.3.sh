#!/usr/bin/env bash
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



set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

. ${SCRIPT_PATH}/start_docker.sh

# remove unnecessary repositories
de bash -c "zypper repos | grep Yes | cut -f2 -d '|' | xargs zypper removerepo"
de zypper --non-interactive remove libapparmor* libgmodule* libX11* libxcb* mozilla* ncurses* qemu* vim*

# add the tumbleweed oss repo
de zypper ar -f -c http://download.opensuse.org/tumbleweed/repo/oss tumbleweed-oss
de zypper --non-interactive --no-gpg-checks update

# install libraries
de zypper --non-interactive install fuse-devel libattr-devel libmysqlclient-devel libuuid-devel pcre-devel

# install extra packages
de zypper --non-interactive install autoconf binutils cmake libgcc_s1 patch pkg-config sudo

if [[ "${C_COMPILER}" = gcc-* ]]; then
    C_PACKAGE="gcc${C_COMPILER##*-}"
    SUSE_C_COMPILER="gcc-${C_COMPILER##*-}"
elif [[ "${C_COMPILER}" = clang-* ]]; then
    C_PACKAGE="clang${C_COMPILER##*-}"
    SUSE_C_COMPILER="clang-${C_COMPILER##*-}.0"
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    CXX_PACKAGE="gcc${CXX_COMPILER##*-}-c++"
    SUSE_CXX_COMPILER="g++-${CXX_COMPILER##*-}"
elif [[ "${CXX_COMPILER}" = clang++-* ]]; then
    CXX_PACKAGE="clang${CXX_COMPILER##*-}"
    SUSE_CXX_COMPILER="clang++-${CXX_COMPILER##*-}.0"
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# install the compilers
# gcc must be installed even if compiling with clang
de zypper --non-interactive install gcc ${C_PACKAGE} ${CXX_PACKAGE}

# add the travis user
de useradd travis -m -s /sbin/nologin || true
de chown -R travis /GUFI

# build and test GUFI
docker exec --env C_COMPILER="${SUSE_C_COMPILER}" --env CXX_COMPILER="${SUSE_CXX_COMPILER}" --env BUILD="${BUILD}" --env CMAKE_FLAGS="${CMAKE_FLAGS}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && ${SCRIPT_PATH}/build_and_test.sh"
