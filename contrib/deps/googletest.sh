#!/usr/bin/env bash
# build and install googletest

set -e

# Assume all paths exist

googletest_name="googletest"
googletest_prefix="${INSTALL_DIR}/${googletest_name}"
if [[ ! -d "${googletest_prefix}" ]]; then
    googletest_build="${BUILD_DIR}/googletest-master"
    if [[ ! -d "${googletest_build}" ]]; then
        googletest_tarball="${DOWNLOAD_DIR}/googletest.tar.gz"
        if [[ ! -f "${googletest_tarball}" ]]; then
            wget https://github.com/google/googletest/archive/master.tar.gz -O "${googletest_tarball}"
        fi

        tar -xf "${googletest_tarball}" -C "${BUILD_DIR}"
    fi

    cd "${googletest_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        cmake .. -DCMAKE_INSTALL_PREFIX="${googletest_prefix}" -DBUILD_GMOCK=OFF
    fi
    make
    make install
fi
