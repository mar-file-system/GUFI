#!/usr/bin/env bash
# build and install googletest

set -e

# Assume all paths exist

googletest_name="googletest"
googletest_prefix="${INSTALL}/${googletest_name}"
if [[ ! -d "${googletest_prefix}" ]]; then
    googletest_build="${BUILD}/googletest-master"
    if [[ ! -d "${googletest_build}" ]]; then
        googletest_tarball="${DOWNLOAD}/googletest.tar.gz"
        if [[ ! -f "${googletest_tarball}" ]]; then
            wget https://github.com/google/googletest/archive/master.tar.gz -O "${googletest_tarball}"
        fi
    fi

    if [[ ! -d "${googletest_build}" ]]; then
        tar -xf "${googletest_tarball}" -C "${BUILD}"
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
