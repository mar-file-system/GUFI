#!/usr/bin/env bash

set -e

export CC="${C_COMPILER}"
export CXX="${CXX_COMPILER}"
export GTEST_COLOR=1
export DEP_PATH="/tmp"
export PATH="${DEP_PATH}/sqlite3/bin:${PATH}"

mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DDEP_INSTALL_PREFIX="${DEP_PATH}" -DPARAMIKO=On ..

# use files from the generated tar
if  [[ "${BUILD}" = "make" ]]; then
    make gary
    mkdir tarball
    tar -xvf gufi.tar.gz -C tarball
    cd tarball
fi

make DEBUG=1

if [[ "${BUILD}" = "cmake" ]]; then
    ctest --verbose
elif [[ "${BUILD}" = "make" ]]; then
    make test
fi
