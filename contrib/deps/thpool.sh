#!/usr/bin/env bash
# build and install C-Thread-Pool

set -e

# Assume all paths exist

thpool_name="C-Thread-Pool"
thpool_prefix="${INSTALL_DIR}/${thpool_name}"
if [[ ! -d "${thpool_prefix}" ]]; then
    thpool_build="${BUILD_DIR}/C-Thread-Pool-lanl"
    if [[ ! -d "${thpool_build}" ]]; then
        thpool_tarball="${DOWNLOAD_DIR}/C-Thread-Pool.tar.gz"
        if [[ ! -f "${thpool_tarball}" ]]; then
            wget https://github.com/mar-file-system/C-Thread-Pool/archive/lanl.tar.gz -O "${thpool_tarball}"
        fi

        tar -xf "${thpool_tarball}" -C "${BUILD_DIR}"
    fi

    cd "${thpool_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        $CMAKE .. -DCMAKE_INSTALL_PREFIX="${thpool_prefix}"
    fi
    make
    make install
fi
