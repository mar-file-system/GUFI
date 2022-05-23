#!/usr/bin/env bash
# build and install sqlite3-pcre

set -e

# install sqlite3 first
${SCRIPT_PATH}/sqlite3.sh

# Assume all paths exist

pcre_name="sqlite3-pcre"
pcre_prefix="${INSTALL_DIR}/${pcre_name}"
if [[ ! -d "${pcre_prefix}" ]]; then
    pcre_build="${BUILD_DIR}/sqlite3-pcre-master"
    if [[ ! -d "${pcre_build}" ]]; then
        pcre_tarball="${DOWNLOAD_DIR}/sqlite3-pcre.tar.gz"
        if [[ ! -f "${pcre_tarball}" ]]; then
            wget https://github.com/mar-file-system/sqlite3-pcre/archive/master.tar.gz -O "${pcre_tarball}"
        fi

        tar -xf "${pcre_tarball}" -C "${BUILD_DIR}"
    fi

    cd "${pcre_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        export PKG_CONFIG_PATH="${INSTALL_DIR}/sqlite3/lib/pkgconfig:${PKG_CONFIG_PATH}"
        ${CMAKE} .. -DCMAKE_INSTALL_PREFIX="${pcre_prefix}"
    fi
    make -j "${THREADS}"
    make install
fi
