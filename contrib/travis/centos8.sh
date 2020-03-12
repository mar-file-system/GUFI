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



SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"

set -e

# install Extra Packages for Enterprise Linux (EPEL) and The Software Collections (SCL) Repository
yum -y install epel-release

# install libraries
yum -y install fuse-devel libattr pcre-devel

# install extra packages
yum -y install autoconf cmake3 diffutils fuse make patch pkgconfig python2-pip

if [[ "${C_COMPILER}" = gcc-* ]]; then
    VERSION="${C_COMPILER##*-}"
    C_PACKAGE="gcc-toolset-${VERSION}"
    CENTOS_C_COMPILER="/usr/bin/gcc"
elif [[ "${C_COMPILER}" = clang ]]; then
    C_PACKAGE="clang"
    CENTOS_C_COMPILER="/usr/bin/clang"
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    VERSION="${CXX_COMPILER##*-}"
    CXX_PACKAGE="gcc-toolset-${VERSION}-gcc-c++"
    CENTOS_CXX_COMPILER="/usr/bin/g++"
elif [[ "${CXX_COMPILER}" = clang++ ]]; then
    CXX_PACKAGE="clang"
    CENTOS_CXX_COMPILER="/usr/bin/clang++"
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# install the compilers
yum -y install ${C_PACKAGE} ${CXX_PACKAGE}

# install xattr
yum -y install python2-devel
yes | pip2 install --user xattr

export C_COMPILER=${CENTOS_C_COMPILER}
export CXX_COMPILER=${CENTOS_CXX_COMPILER}
export LD_LIBRARY_PATH="/opt/rh/httpd24/root/usr/lib64/:${LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="/tmp/sqlite3/lib/pkgconfig:${PKG_CONFIG_PATH}"
export PATH="${HOME}/.local/bin:${PATH}"

# build and test GUFI
${SCRIPT_PATH}/build_and_test.sh
