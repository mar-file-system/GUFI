#!/usr/bin/env bash

# get where this script is
SCRIPT_PATH="$(realpath $(dirname ${BASH_SOURCE[0]}))"

#Call the right cmake binary
#!/bin/bash

#Call the right cmake binary
if [ -x "$(command -v cmake)" ]
 then
  CMAKE=$(command -v cmake)
elif [ -x "$(command -v cmake3)" ]
 then
  CMAKE=$(command -v cmake3)
else
  echo "No cmake available!"
  exit 0
fi


set -e

# dependency download path
DOWNLOAD="$(realpath $1)"
mkdir -p "${DOWNLOAD}"

BUILD="$(realpath $2)"
mkdir -p "${BUILD}"

# dependency install path
INSTALL="$(realpath $3)"
mkdir -p "${INSTALL}"

# build and install C-Thread-Pool
thpool_tarball="${DOWNLOAD}/C-Thread-Pool.tar.gz"
if [[ ! -f "${thpool_tarball}" ]]; then
    wget https://github.com/mar-file-system/C-Thread-Pool/archive/lanl.tar.gz -O "${thpool_tarball}"
fi

thpool_name="C-Thread-Pool"
thpool_prefix="${INSTALL}/${thpool_name}"
if [[ ! -d "${thpool_prefix}" ]]; then
    if [[ ! -d "${thpool_tarball}" ]]; then
        tar -xf "${thpool_tarball}" -C "${BUILD}"
    fi
    cd "${BUILD}/C-Thread-Pool-lanl"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        $CMAKE .. -DCMAKE_INSTALL_PREFIX="${thpool_prefix}"
    fi
    make
    make install
fi

# build and install sqlite3
sqlite3_tarball="${DOWNLOAD}/sqlite-autoconf-3270200.tar.gz"
if [[ ! -f "${sqlite3_tarball}" ]]; then
    wget https://www.sqlite.org/2019/sqlite-autoconf-3270200.tar.gz -O "${sqlite3_tarball}"
fi

sqlite3_name="sqlite3"
sqlite3_prefix="${INSTALL}/${sqlite3_name}"
if [[ ! -d "${sqlite3_prefix}" ]]; then
    if [[ ! -d "${BUILD}/sqlite-autoconf-3270200" ]]; then
        tar -xf "${sqlite3_tarball}" -C "${BUILD}"
    fi
    cd "${BUILD}/sqlite-autoconf-3270200"
    patch < "${SCRIPT_PATH}/sqlite-autoconf-3270200.patch"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        ../configure --prefix="${sqlite3_prefix}"
    fi
    make
    make install
fi

# build and install sqlite3-pcre
pcre_tarball="${DOWNLOAD}/sqlite3-pcre.tar.gz"
if [[ ! -f "${pcre_tarball}" ]]; then
    wget https://github.com/mar-file-system/sqlite3-pcre/archive/master.tar.gz -O "${pcre_tarball}"
fi

pcre_name="sqlite3-pcre"
pcre_prefix="${INSTALL}/${pcre_name}"
if [[ ! -d "${pcre_prefix}" ]]; then
    export PKG_CONFIG_PATH="${sqlite3_prefix}/lib/pkgconfig:${PKG_CONFIG_PATH}"
    if [[ ! -d "${BUILD}/sqlite3-pcre-master" ]]; then
        tar -xf "${pcre_tarball}" -C "${BUILD}"
    fi
    cd "${BUILD}/sqlite3-pcre-master"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        $CMAKE .. -DCMAKE_INSTALL_PREFIX="${pcre_prefix}"
    fi
    make
    make install
fi
