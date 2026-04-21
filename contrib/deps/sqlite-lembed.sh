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



# build and install sqlite-vec

set -e

CYGWIN="$1"
OMP_FLAGS="$2"

# install sqlite3 first
"${SCRIPT_PATH}/sqlite3.sh"

# Assume all paths exist

lembed_name="sqlite-lembed"
lembed_prefix="${INSTALL_DIR}/${lembed_name}"
llama_name="llama.cpp"
llama_prefix="${INSTALL_DIR}/${llama_name}"
if [[ ! -d "${lembed_prefix}" ]]; then
    lembed_build="${BUILD_DIR}/sqlite-lembed-main"
    if [[ ! -d "${lembed_build}" ]]; then
        lembed_tarball="${DOWNLOAD_DIR}/sqlite-lembed.tar.gz"
        if [[ ! -f "${lembed_tarball}" ]]; then
            wget https://github.com/asg017/sqlite-lembed/archive/refs/heads/main.tar.gz -O "${lembed_tarball}"
        fi

        tar -xf "${lembed_tarball}" -C "${BUILD_DIR}"
        patch -p1 -d "${lembed_build}" < "${SCRIPT_PATH}/sqlite-lembed.patch"

        rm -r "${lembed_build}/vendor/llama.cpp"
    fi

    llama_build="${lembed_build}/vendor/llama.cpp"
    if [[ ! -f "${llama_build}/Makefile" ]]; then
        llama_tarball="${DOWNLOAD_DIR}/llama.cpp-2b33896.tar.gz"
        if [[ ! -f "${llama_tarball}" ]]; then
            wget https://github.com/ggml-org/llama.cpp/archive/2b3389677a833cee0880226533a1768b1a9508d2.tar.gz -O "${llama_tarball}"
            tar -xf "${llama_tarball}" -C "${lembed_build}/vendor/"
            mv "${lembed_build}/vendor/llama.cpp-2b3389677a833cee0880226533a1768b1a9508d2" "${llama_build}"
        else
            mkdir -p "${llama_build}"
            tar -xf "${llama_tarball}" -C "${llama_build}"
            if [[ "${CYGWIN}" == "true" ]]; then
                patch -p1 -d "${llama_build}" < "${SCRIPT_PATH}/llama.cpp-2b33896.patch"
            fi
        fi
    fi

    cd "${llama_build}"
    mkdir -p build
    cd build
    CC="${CC}" CXX="${CXX}" CXXFLAGS="-I${INSTALL_DIR}/sqlite3" "${CMAKE}" .. -DCMAKE_INSTALL_PREFIX="${llama_prefix}" -DCMAKE_INSTALL_LIBDIR=lib -DLLAMA_METAL=OFF ${OMP_FLAGS} # not quoting OMP_FLAGS
    make -j "${THREADS}"
    make -j "${THREADS}" install

    cd "${lembed_build}"
    sed -i "s%@DEP_INSTALL_PREFIX@%${INSTALL_DIR}%g;" CMakeLists.txt

    make sqlite-lembed.h
    make static loadable
    mkdir -p "${lembed_prefix}/include" "${lembed_prefix}/lib"
    cp sqlite-lembed.h "${lembed_prefix}/include"
    cp dist/libsqlite_lembed0.* dist/lembed0.* "${lembed_prefix}/lib"
fi
