#!/usr/bin/env bash
# build and install jemalloc

set -e

# Assume all paths exist

jemalloc_name="jemalloc"
jemalloc_prefix="${INSTALL_DIR}/${jemalloc_name}"
if [[ ! -d "${jemalloc_prefix}" ]]; then
    jemalloc_build="${BUILD_DIR}/jemalloc-master"
    if [[ ! -d "${jemalloc_build}" ]]; then
        jemalloc_tarball="${DOWNLOAD_DIR}/jemalloc.tar.gz"
        if [[ ! -f "${jemalloc_tarball}" ]]; then
            wget https://github.com/jemalloc/jemalloc/archive/master.tar.gz -O "${jemalloc_tarball}"
        fi

        tar -xf "${jemalloc_tarball}" -C "${BUILD_DIR}"
    fi

    cd "${jemalloc_build}"
    ./autogen.sh
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        ../configure --prefix="${jemalloc_prefix}"
    fi
    make -j "${THREADS}"
    make -i install
fi
