#!/usr/bin/env bash
# build and install sqlite3

set -e

# Assume all paths exist

sqlite3_name="sqlite3"
sqlite3_prefix="${INSTALL_DIR}/${sqlite3_name}"
if [[ ! -d "${sqlite3_prefix}" ]]; then
    sqlite3_build="${BUILD_DIR}/sqlite-autoconf-3270200"
    if [[ ! -d "${sqlite3_build}" ]]; then
        sqlite3_tarball="${DOWNLOAD_DIR}/sqlite-autoconf-3270200.tar.gz"
        if [[ ! -f "${sqlite3_tarball}" ]]; then
            wget https://www.sqlite.org/2019/sqlite-autoconf-3270200.tar.gz -O "${sqlite3_tarball}"
        fi

        tar -xf "${sqlite3_tarball}" -C "${BUILD_DIR}"
        patch -p1 -d "${sqlite3_build}" < "${SCRIPT_PATH}/sqlite-autoconf-3270200.patch"
    fi

    cd "${sqlite3_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        CFLAGS="-USQLITE_THREADSAFE -DSQLITE_THREADSAFE=0 -USQLITE_MAX_EXPR_DEPTH -DSQLITE_MAX_EXPR_DEPTH=0" ../configure --prefix="${sqlite3_prefix}"
    fi
    make
    make install
fi
