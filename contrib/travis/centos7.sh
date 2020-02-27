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

# install Extra Packages for Enterprise Linux (EPEL) and The Software Collections (SCL) Repository
de yum -y install epel-release centos-release-scl

# install libraries
de yum -y install fuse-devel libattr1 pcre-devel

# install extra packages
de yum -y install autoconf cmake3 make patch pkgconfig

# create symlinks
de ln -sf /usr/bin/cmake3 /usr/bin/cmake
de ln -sf /usr/bin/ctest3 /usr/bin/ctest

if [[ "${C_COMPILER}" = gcc-* ]]; then
    VERSION="${C_COMPILER##*-}"
    C_PACKAGE="devtoolset-${VERSION}"
    CENTOS_C_COMPILER="gcc-${VERSION}"
    de update-alternatives --install /usr/bin/gcc-${VERSION} gcc-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/gcc 10
elif [[ "${C_COMPILER}" = clang ]]; then
    # llvm-toolset-7 installs clang-5.0
    C_PACKAGE="llvm-toolset-7"
    CENTOS_C_COMPILER="clang-5.0"
    de update-alternatives --install /usr/bin/clang-5.0 clang-5.0 /opt/rh/llvm-toolset-7/root/usr/bin/clang-5.0 10
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    VERSION="${CXX_COMPILER##*-}"
    CXX_PACKAGE="devtoolset-${VERSION}-gcc-c++"
    CENTOS_CXX_COMPILER="g++-${VERSION}"
    de update-alternatives --install /usr/bin/g++-${VERSION} g++-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/g++ 10
elif [[ "${CXX_COMPILER}" = clang++ ]]; then
    # llvm-toolset-7 installs clang-5.0
    CXX_PACKAGE="llvm-toolset-7"
    CENTOS_CXX_COMPILER="clang++"
    de update-alternatives --install /usr/bin/clang++ clang++ /opt/rh/llvm-toolset-7/root/usr/bin/clang++ 10
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# install the compilers
de yum -y install ${C_PACKAGE} ${CXX_PACKAGE}

# build and test GUFI
docker exec --env C_COMPILER="${CENTOS_C_COMPILER}" --env CXX_COMPILER="${CENTOS_CXX_COMPILER}" --env BUILD="${BUILD}" --env CMAKE_FLAGS="${CMAKE_FLAGS}" "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && LD_LIBRARY_PATH=\"/opt/rh/httpd24/root/usr/lib64/:\$(printenv LD_LIBRARY_PATH)\" PKG_CONFIG_PATH=\"/tmp/sqlite3/lib/pkgconfig:\$(printenv PKG_CONFIG_PATH)\" ${SCRIPT_PATH}/build_and_test.sh"
