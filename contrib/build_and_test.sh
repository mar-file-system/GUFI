#!/usr/bin/env bash

set -e

export CC="${C_COMPILER}"
export CXX="${CXX_COMPILER}"
export GTEST_COLOR=1

mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX=install -DCMAKE_BUILD_TYPE=Debug ..

# use files from the generated tar
if  [[ "${BUILD}" = "make" ]]; then
    make gary
    cp utils.c ..
    cd ..
    tar -xf build/gufi.tar.gz C-Thread-Pool sqlite3-pcre googletest
fi

make DEBUG=1

if [[ "${BUILD}" = "cmake" ]]; then
    ctest --verbose
elif [[ "${BUILD}" = "make" ]]; then
    make test
fi
